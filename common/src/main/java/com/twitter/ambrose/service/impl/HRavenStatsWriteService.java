package com.twitter.ambrose.service.impl;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.mapred.JobConf;

import com.google.common.base.Preconditions;
import com.twitter.ambrose.model.DAGNode;
import com.twitter.ambrose.model.Event;
import com.twitter.ambrose.model.Job;
import com.twitter.ambrose.model.WorkflowId;
import com.twitter.ambrose.service.StatsWriteService;
import com.twitter.ambrose.util.JSONUtil;
import com.twitter.hraven.Flow;
import com.twitter.hraven.FlowEvent;
import com.twitter.hraven.FlowEventKey;
import com.twitter.hraven.FlowKey;
import com.twitter.hraven.FlowQueueKey;
import com.twitter.hraven.Framework;
import com.twitter.hraven.JobDescFactory;
import com.twitter.hraven.datasource.FlowEventService;
import com.twitter.hraven.datasource.FlowQueueService;

public class HRavenStatsWriteService implements StatsWriteService {
  private static final Log LOG = LogFactory.getLog(HRavenStatsWriteService.class);
  private final String username;
  private final Set<String> runningJobs;
  private final Set<String> completedJobs;
  private final Set<String> failedJobs;
  private FlowQueueService flowQueueService;
  private FlowEventService flowEventService;
  private final ExecutorService hRavenPool;
  private boolean initialized = false;
  private String cluster;
  private String appId;
  private FlowKey flowKey;
  private FlowQueueKey flowQueueKey;
  private Map<String, DAGNode<Job>> dagNodeNameMap;
  private static final int HRAVEN_POOL_SHUTDOWN_SECS = 5;
  private JobConf jobConf;

  public HRavenStatsWriteService() {
    runningJobs = new HashSet<String>();
    completedJobs = new HashSet<String>();
    failedJobs = new HashSet<String>();
    username = System.getProperty("user.name");

    // queue hRaven requests up and fire them asynchronously
    hRavenPool = Executors.newFixedThreadPool(1);

    // we try to shut down gracefully, but this exists if we can't
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        shutdown();
      }
    });
  }

  static Configuration initHBaseConfiguration(Configuration jobConf) {
    // setup hraven hbase connection information
    Configuration hbaseConf = null;
    String hravenDir = System.getenv(HRAVEN_HBASE_CONF_DIR_ENV);
    if (hravenDir != null && !hravenDir.isEmpty()) {
      hbaseConf = new Configuration(false);
      Path hbaseConfFile = new Path(hravenDir, HRAVEN_HBASE_CONF_FILE);
      LOG.info("Loading hRaven HBase configuration from " + hbaseConfFile.toString());
      hbaseConf.addResource(hbaseConfFile);
    } else {
      hbaseConf = HBaseConfiguration.create();
      // override any hRaven connection properties from the job
      String hbaseZKQuorum = jobConf.get(HRAVEN_ZOOKEEPER_QUORUM);
      if (hbaseZKQuorum != null && !hbaseZKQuorum.isEmpty()) {
        try {
          LOG.info("Applying hRaven Zookeeper quorum information from job conf: " + hbaseZKQuorum);
          ZKUtil.applyClusterKeyToConf(hbaseConf, hbaseZKQuorum);
        } catch (IOException ioe) {
          throw new RuntimeException("Invalid cluster configuration for "
              + HRAVEN_ZOOKEEPER_QUORUM + " (\"" + hbaseZKQuorum + "\")");
        }
      } else if (LOG.isDebugEnabled()) {
        LOG.debug("No cluster configuration found for " + HRAVEN_ZOOKEEPER_QUORUM
            + ", continuing with default");
      }
    }

    return hbaseConf;
  }

  /* Configuration keys for hRaven HBase storage
   *
   * We construct an HBase configuration to use for the hRaven client using the following logic:
   * 1) if the HRAVEN_HBASE_CONF_DIR environment variable is set, we construct a Configuration
   *    object and add the file $HRAVEN_HBASE_CONF_DIR/hbase-site.xml
   * 2) otherwise, we look in the job configuration for a property named
   *    "hraven.hbase.zookeeper.quorum", (in the format
   *    "quorum peer hostname1[,quorum peer host2,...]:client port:parent znode").  If found, this
   *    value is split and applied to the HBase configuration.
   */
  /** Environment variable pointing to the configuration directory to use with hRaven */
  public static final String HRAVEN_HBASE_CONF_DIR_ENV = "HRAVEN_HBASE_CONF_DIR";
  /** Filename to load the HBase configuration from */
  public static final String HRAVEN_HBASE_CONF_FILE = "hbase-site.xml";

  /** Connection information for the hRaven HBase zookeeper quorum,
   * ("quorum peer hostname1[,quorum peer host2,...]:client port:parent znode"). */
  public static final String HRAVEN_ZOOKEEPER_QUORUM = "hraven." + HConstants.ZOOKEEPER_QUORUM;

  /** The url for Ambrose dashboard. */
  private static final String AMBROSE_URL =
      "http://ambrose.prod.analytics.service.smf1.twitter.com";
  private static final String AMBROSE_WORKFLOW_PARAM = "/workflow.html?workflowId=";

  private static final class HRavenEventRunnable implements Runnable {

    private final FlowEventService flowEventService;
    private final FlowEvent flowEvent;

    private HRavenEventRunnable(FlowEventService flowEventService, FlowEvent flowEvent) {
      this.flowEventService = flowEventService;
      this.flowEvent = flowEvent;
    }

    @Override
    public void run() {
      try {
        LOG.info("Submitting flowEvent to hRaven: " + flowEvent.getFlowEventKey().getSequence());
        flowEventService.addEvent(flowEvent);
      } catch (Exception e) {
        LOG.warn("Error making request to HRaven FlowEventService for event flowEventKey: "
            + flowEvent.getFlowEventKey(), e);
      }
    }
  }

  private final class HRavenQueueRunnable implements Runnable {
    private final FlowQueueService flowQueueService;
    private final FlowQueueKey flowQueueKey;
    private FlowQueueKey newQueueKey;
    private Flow flow;

    private HRavenQueueRunnable(FlowQueueService flowQueueService,
      FlowQueueKey flowQueueKey,
      Flow flow) {
      this.flowQueueService = flowQueueService;
      this.flowQueueKey = flowQueueKey;
      this.flow = flow;
    }

    private HRavenQueueRunnable(FlowQueueService flowQueueService,
      FlowQueueKey flowQueueKey,
      FlowQueueKey newQueueKey) {
      this.flowQueueService = flowQueueService;
      this.flowQueueKey = flowQueueKey;
      this.newQueueKey = newQueueKey;
    }

    @Override
    public void run() {
      try {
        if (flow != null) {
          LOG.debug("Submitting update to flowQueue to hRaven: " + flowQueueKey.getFlowId());

          flowQueueService.updateFlow(flowQueueKey, flow);

          LOG.debug("Submitted update to flowQueue to hRaven: " + flowQueueKey.getFlowId());

        } else if (newQueueKey != null) {
          LOG.info(String.format(
              "Submitting request to hRaven to move flowQueueKey from %s to %s",
              flowQueueKey != null ? flowQueueKey : "", newQueueKey));

          flowQueueService.moveFlow(flowQueueKey, newQueueKey);

          LOG.info(String.format(
              "Submitted request to hRaven to move flowQueueKey from %s to %s",
              flowQueueKey != null ? flowQueueKey : "", newQueueKey));

        } else {
          LOG.warn("Can not make request to flowQueueService since it is not clear if this is an "
              + "update or a move, since both flow and newQueueKey are null");
        }
      } catch (Exception e) {
        LOG.error("Error making request to HRaven FlowQueueService for flowQueueKey: "
            + flowQueueKey, e);
      }
    }
  }

  /**
   * Check if all the Set and Map used to track the flow is initialized.
   * @return boolean variable to indicate that the variables are all initialized.
   */
  private boolean ifInitialized() {
    return runningJobs != null && completedJobs != null && failedJobs != null
        && dagNodeNameMap != null;
  }

  public void shutdown() {
    if (ifInitialized() && (runningJobs.size() > 0
        || completedJobs.size() + failedJobs.size() < dagNodeNameMap.size())) {
      try {
        changeFlowQueueStatus(Flow.Status.FAILED);
      } catch (IOException e) {
        LOG.error("Couldn't change queue status to failed", e);
      }
    }

    hRavenPool.shutdown();
    try {
      LOG.info(String.format(
          "Waiting for up to %d seconds for any pending hRavenPool requests to complete",
          HRAVEN_POOL_SHUTDOWN_SECS));

      hRavenPool.awaitTermination(HRAVEN_POOL_SHUTDOWN_SECS, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      LOG.error("Was not able to await termination for hRavenPool", e);
    }
  }

  /**
   * Change the FlowQueue to a new status
   *
   * @param status status to change the queue key to
   * @throws IOException if things go bad
   */
  private void changeFlowQueueStatus(Flow.Status status) throws IOException {
    LOG.info(String.format("Changing flowQueueStatus from %s to %s",
        flowQueueKey.getStatus(), status));

    FlowQueueKey newQueueKey = new FlowQueueKey(flowQueueKey.getCluster(), status,
        flowQueueKey.getTimestamp(), flowQueueKey.getFlowId());
    hRavenPool.submit(new HRavenQueueRunnable(flowQueueService, flowQueueKey, newQueueKey));
    flowQueueKey = newQueueKey;
  }

  private void lazyInitWorkflow() {
    if (initialized) {
      return;
    }
    initializeWorkflow();
  }

  private synchronized void initializeWorkflow() {
    if (initialized) {
      return;
    }
    initialized = true;

    Configuration jobConf = new Configuration();

    // setup hraven hbase connection information
    Configuration hbaseConf = initHBaseConfiguration(jobConf);

    try {
      flowQueueService = new FlowQueueService(hbaseConf);
    } catch (IOException e) {
      throw new RuntimeException("Could not instantiate hRaven FlowQueueService", e);
    }

    try {
      flowEventService = new FlowEventService(hbaseConf);
    } catch (IOException e) {
      throw new RuntimeException("Could not instantiate hRaven FlowEventService", e);
    }

    cluster = JobDescFactory.getCluster(jobConf);
    if (cluster == null) {
      cluster = "default";
    }

    // This will return appid for pig, cascading consistent with hraven (only available in 0.9.13+)
    appId = JobDescFactory.getFrameworkSpecificJobDescFactory(jobConf).getAppId(jobConf);

    // TODO runID could match with hraven's runId for easy navigation between two systems
    flowKey = new FlowKey(cluster, username, appId, System.currentTimeMillis());

    // create a new flow queue entry
    UUID uuid = UUID.randomUUID();
    flowQueueKey = new FlowQueueKey(cluster, Flow.Status.RUNNING,
        System.currentTimeMillis(), uuid.toString());

    // Log the Ambrose workflow url.
    WorkflowId workflowId = new WorkflowId(cluster, username,
        appId, flowKey.getRunId(), flowQueueKey.getTimestamp(),
        flowQueueKey.getFlowId());
    LOG.info("Ambrose workflow page: " +  AMBROSE_URL
        + AMBROSE_WORKFLOW_PARAM + workflowId.toId());

  }

  @Override
  public void sendDagNodeNameMap(String workflowId,
      Map dagNodeMap) throws IOException {
    this.dagNodeNameMap = dagNodeMap;
    lazyInitWorkflow();
    updateFlowQueue(flowQueueKey);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void pushEvent(String workflowId, Event event) throws IOException {
    lazyInitWorkflow();

    String eventDataJson = event.toJson();
    switch (event.getType()) {
      case WORKFLOW_PROGRESS:
        updateWorkflowProgress((Map<Event.WorkflowProgressField, String>) event.getPayload());
        break;
      case JOB_STARTED:
        updateJobStarted((DAGNode) event.getPayload());
        break;
      case JOB_FAILED:
      case JOB_FINISHED:
        updateJobComplete((DAGNode) event.getPayload(), event.getType());
        break;
      default:
        break;
    }

    Preconditions.checkNotNull(flowKey, String.format(
        "Can not push event type %s because flowKey is not set", event.getType()));
    FlowEventKey eventKey = new FlowEventKey(flowKey, event.getId());
    FlowEvent flowEvent = new FlowEvent(eventKey);
    flowEvent.setTimestamp(event.getTimestamp());
    flowEvent.setFramework(JobDescFactory.getFramework(jobConf));
    flowEvent.setType(event.getType().name());

    if (eventDataJson != null) {
      flowEvent.setEventDataJSON(eventDataJson);
    }

    hRavenPool.submit(new HRavenEventRunnable(flowEventService, flowEvent));
  }

  /**
   * Repersist the FlowQueue for this object.
   *
   * @param key key to the queue
   * @throws IOException if things go bad
   */
  private void updateFlowQueue(FlowQueueKey key) throws IOException {
    updateFlowQueue(key, null);
  }

  private void updateFlowQueue(FlowQueueKey key, Integer progress) throws IOException {
    Flow flow = new Flow(flowKey);
    if (progress != null) {
      flow.setProgress(progress);
    }
    flow.setQueueKey(key);
    flow.setFlowName(appId);
    flow.setUserName(username);
    flow.setJobGraphJSON(JSONUtil.toJson(dagNodeNameMap));
    hRavenPool.submit(new HRavenQueueRunnable(flowQueueService, flowQueueKey, flow));
  }

  private void updateWorkflowProgress(Map<Event.WorkflowProgressField, String> progressMap)
      throws IOException {
    int progress = Integer.parseInt(
        progressMap.get(Event.WorkflowProgressField.workflowProgress));

    if (progress == 100) {
      updateWorkflowStateIfDone();
    }

    updateFlowQueue(flowQueueKey, progress);
  }

  private String updateJobStarted(DAGNode node) throws IOException {
    runningJobs.add(node.getJob().getId());
    return node.toJson();
  }

  private void updateJobComplete(DAGNode node, Event.Type eventType)
      throws IOException {
    String jobId = node.getJob().getId();
    switch (eventType) {
      case JOB_FINISHED:
        runningJobs.remove(jobId);
        completedJobs.add(jobId);
        break;
      case JOB_FAILED:
        runningJobs.remove(jobId);
        failedJobs.add(jobId);
        break;
      default:
        throw new IllegalArgumentException(
            "Unrecognized Event type in updateJobInfo: " + eventType);
    }

    updateWorkflowStateIfDone();
  }

  private void updateWorkflowStateIfDone() throws IOException {
    // see if we're done or not. We're done if no jobs are running and the sum of completed jobs
    // is the size of the DAG
    if (runningJobs.size() > 0 || dagNodeNameMap.size() == 0
        || (completedJobs.size() + failedJobs.size() != dagNodeNameMap.size())) {
      return;
    }

    LOG.info(String.format("Ambrose total jobs: %d, succeeded: %d, failed: %d",
        dagNodeNameMap.size(), completedJobs.size(), failedJobs.size()));

    if (failedJobs.size() > 0) {
      //change queue state to failed
      changeFlowQueueStatus(Flow.Status.FAILED);
    } else {
      //change queue state to succeeded
      changeFlowQueueStatus(Flow.Status.SUCCEEDED);
    }

    // Calling this here results in a RejectedExecutionException for the above submit calls,
    // which is odd becase they've already been submitted
    //shutdown();
  }
}

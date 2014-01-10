/*
Copyright 2012 Twitter, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package com.twitter.ambrose.service.impl;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Preconditions;

import com.twitter.hraven.*;
import com.twitter.hraven.util.StringUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;


import com.twitter.hraven.datasource.FlowEventService;
import com.twitter.hraven.datasource.FlowQueueService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.ambrose.model.DAGNode;
import com.twitter.ambrose.model.Event;
import com.twitter.ambrose.model.Job;
import com.twitter.ambrose.model.WorkflowId;
import com.twitter.ambrose.service.StatsWriteService;
import com.twitter.ambrose.util.JSONUtil;

/** StatsWriteService to send statistics to HRaven
  * Requires that HRAVEN_HBASE_CONF_DIR environment variable be set to point to 
  * to a directory with hbase-site.xml for configuration.
  * Also, JoobConf must contain a batch.desc property for the job name, that will
  * be used as a part of workflowid. 
  * The subclass and override getJobConf() to return a configuration object (for lazy
  * initialization) or use @link HRavenStatsWriteService#forJob to instantiate a 
  * service given a config.
  */
public abstract class HRavenStatsWriteService implements StatsWriteService<Job> {

   private static final int HRAVEN_POOL_SHUTDOWN_SECS = 5;

   private static final Logger LOG = LoggerFactory.getLogger(InMemoryStatsService.class);
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
   private Configuration jobConf;

  // Callable so we can invoke HRaven in a separate thread
  private final class HRavenEventRunnable implements Runnable {
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
          LOG.error("Error making request to HRaven FlowEventService for event flowEventKey: "
                + flowEvent.getFlowEventKey(), e);
        }
     }
  }

  // Callable so we can invoke HRaven in a separate thread
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

        // Log the Ambrose workflow url.
        WorkflowId workflowId = new WorkflowId(flow.getCluster(), flow.getUserName(),
             flow.getFlowKey().getAppId(), flow.getRunId(), flow.getQueueKey().getTimestamp(),
             flow.getQueueKey().getFlowId());

           /* TODO:
           LOG.info("Ambrose workflow page: " +  AMBROSE_URL
                + AMBROSE_WORKFLOW_PARAM + workflowId.toId());
                */
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

  protected HRavenStatsWriteService() {
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

  /**
  * Check if all the Set and Map used to track the flow is initialized.
  * @return boolean variable to indicate that the variables are all initialized.
  */
  private boolean ifInitialized() {
     return initialized && runningJobs != null && completedJobs != null && failedJobs != null
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

   protected  abstract Configuration getJobConf();
   private void lazyInit() {
      if (initialized) {
        return;
      }
      try {
        initialize(getJobConf());
      }
      catch(RuntimeException e) {
        LOG.error("Fatal exception initializing listener: ", e);
        throw e;
      }
   }

   private synchronized void initialize(Configuration jobConf) {
      if (initialized) {
        return;
      }

      // setup hraven hbase connection information
      Configuration hbaseConf = initHBaseConfiguration(jobConf);

      LOG.info("Ambrose listener is initializing: jobConf=" + jobConf + ";\nhbaseConf=" + hbaseConf);

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
        cluster = "dw@smf1";
      } // no cluster found when run in local mode

      appId = jobConf.get(Constants.APP_NAME_CONF_KEY);

      // TODO: we need to refactor this out of here and into hRaven to assure we have consistent
      // appIds. See https://jira.twitter.biz/browse/HRAV-90
      // if batch.desc isn't set, try to parse it from mapred.job.name or jobName
      if (appId == null) {
        appId = jobConf.get(Constants.APP_NAME_CONF_KEY,
              jobConf.get("mapred.job.name",
                   jobConf.get("jobName")));
        Matcher matcher = Pattern.compile(
              Constants.PIG_SCHEDULED_JOBNAME_PATTERN_REGEX).matcher(appId);
        if (matcher.matches()) {
           appId = matcher.group(1);
        }
      }

      appId = appId != null ? StringUtil.cleanseToken(appId) : Constants.UNKNOWN;

      flowKey = new FlowKey(cluster, username, appId, System.currentTimeMillis());

      // create a new flow queue entry
      UUID uuid = UUID.randomUUID();
      flowQueueKey = new FlowQueueKey(cluster, Flow.Status.RUNNING,
           System.currentTimeMillis(), uuid.toString());
      initialized = true;
   }

   @Override
   public void sendDagNodeNameMap(String workflowId, Map<String, DAGNode<Job>> dagNodeMap) throws IOException {
      this.dagNodeNameMap = dagNodeMap;
      lazyInit();
      updateFlowQueue(flowQueueKey);
   }

   @SuppressWarnings("unchecked")
   @Override
   public void pushEvent(String workflowId, Event event) throws IOException {
      lazyInit();

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
      flowEvent.setFramework(Framework.PIG);
      flowEvent.setType(event.getType().name());

      if (eventDataJson != null) {
        flowEvent.setEventDataJSON(eventDataJson);
      }

      hRavenPool.submit(new HRavenEventRunnable(flowEventService, flowEvent));
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

    private void updateWorkflowProgress(Map<Event.WorkflowProgressField, String> progressMap) throws IOException {
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

    private void updateJobComplete(DAGNode node, Event.Type eventType) throws IOException {
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

    /** Environment variable pointing to the configuration directory to use with hRaven */
    public static final String HRAVEN_HBASE_CONF_DIR_ENV = "HRAVEN_HBASE_CONF_DIR";
    /** Filename to load the HBase configuration from */
    public static final String HRAVEN_HBASE_CONF_FILE = "hbase-site.xml";

    /** Connection information for the hRaven HBase zookeeper quorum,
    * ("quorum peer hostname1[,quorum peer host2,...]:client port:parent znode"). */
    public static final String HRAVEN_ZOOKEEPER_QUORUM = "hraven." + HConstants.ZOOKEEPER_QUORUM;


    protected  Configuration initHBaseConfiguration(Configuration jobConf) {
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

   public static StatsWriteService forJob(final Configuration jobConf) {
      return new HRavenStatsWriteService() {
        @Override protected Configuration getJobConf() { return jobConf; }
      };
   }
}

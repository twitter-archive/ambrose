package com.twitter.ambrose.hive;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.twitter.ambrose.model.DAGNode;
import com.twitter.ambrose.model.Event;
import com.twitter.ambrose.model.Job;
import com.twitter.ambrose.server.ScriptStatusServer;
import com.twitter.ambrose.service.impl.InMemoryStatsService;

/**
 * Stateful singleton class which maintains a shared global state between hooks.
 * It collects job information and job/workflow status reports from the hooks 
 * and passes them to an Ambrose StatsWriteService object during the life cycle of 
 * the running Hive script.
 * 
 * @author Lorand Bendig <lbendig@gmail.com>
 *
 */
public enum HiveProgressReporter {
    
    INSTANCE;

    private static final Log LOG = LogFactory.getLog(HiveProgressReporter.class);
   
    private final InMemoryStatsService service;
    private final ScriptStatusServer server;
    
    /** DAG and workflow progress shared between ClientStatsPublisher threads */
    private Map<String, DAGNode<Job>> nodeIdToDAGNode;
    private Map<String, Integer> jobIdToProgress;
    private Map<String, String> jobIdToNodeId;
    private List<Job> jobs;
    private Set<String> completedJobIds;
    
    private int totalMRJobs;
    private String workflowVersion;

    /** holds all events within a script (for all workflows) */
    private SortedMap<Integer, Event<?>> allEvents = new ConcurrentSkipListMap<Integer, Event<?>>();
    
    /** holds all dagNodes within a script (for all workflows) */
    private SortedMap<String, DAGNode<Job>> allDagNodes = new ConcurrentSkipListMap<String, DAGNode<Job>>();
    
    /** internal eventMap field unfolded from from InMemoryStatsService */
    private SortedMap<Integer, Event<?>> _eventMap;
    
    static enum JobProgressField {
        jobId,
        jobName, 
        trackingUrl, 
        isComplete, 
        isSuccessful, 
        mapProgress, 
        reduceProgress, 
        totalMappers, 
        totalReducers;
    }

    @SuppressWarnings("unchecked")
    private HiveProgressReporter() {
        service = new InMemoryStatsService();
        server = new ScriptStatusServer(service, service);
        init();
        server.start();
        initInternal();
    }
    
    private void init() {
        jobIdToProgress = new ConcurrentHashMap<String, Integer>();
        jobIdToNodeId = new ConcurrentHashMap<String, String>();
        jobs = new CopyOnWriteArrayList<Job>();
        completedJobIds = new CopyOnWriteArraySet<String>();
        totalMRJobs = 0;
        workflowVersion = null;
    }
    
    @SuppressWarnings("unchecked")
    private void initInternal() {
        try {
            Field eventMapField = 
                AmbroseHiveUtil.getInternalField(InMemoryStatsService.class, "eventMap");
            _eventMap = (SortedMap<Integer, Event<?>>) eventMapField.get(service);
        }
        catch (Exception e) {
            LOG.fatal("Can't access to eventMap/dagNodeNameMap fields at "
                    + InMemoryStatsService.class.getName() + "!");
            throw new RuntimeException("Incompatible Hive API found!", e);
        }
    }
    
    public static HiveProgressReporter get() {
        return INSTANCE;
    }
    
    public static void reset() {
        INSTANCE.init();
        INSTANCE._eventMap.clear();
        INSTANCE.nodeIdToDAGNode= new ConcurrentSkipListMap<String, DAGNode<Job>>();
        INSTANCE.sendDagNodeNameMap(null, INSTANCE.nodeIdToDAGNode);
    }

    public Map<String, DAGNode<Job>> getNodeIdToDAGNode() {
        return nodeIdToDAGNode;
    }

    public void setNodeIdToDAGNode(Map<String, DAGNode<Job>> nodeIdToDAGNode) {
        this.nodeIdToDAGNode = nodeIdToDAGNode;
    }

    public void addJobIdToProgress(String jobID, int progressUpdate) {
        jobIdToProgress.put(jobID, progressUpdate);
    }

    public Map<String, String> getJobIdToNodeId() {
        return jobIdToNodeId;
    }

    public void addJobIdToNodeId(String jobId, String nodeId) {
        jobIdToNodeId.put(jobId, nodeId);
    }
    
    public DAGNode<Job> getDAGNodeFromNodeId(String nodeId) {
        return nodeIdToDAGNode.get(nodeId);
    }
    
    /**
     * Overall progress of the submitted script
     * @return a number between 0 and 100
     */
    public synchronized int getOverallProgress() {
        int sum = 0;
        for (int progress : jobIdToProgress.values()) {
            sum += progress;
        }
        return sum / totalMRJobs;
    }

    public void addJob(Job job) {
        jobs.add(job);
    }

    public List<Job> getJobs() {
        return jobs;
    }

    public String getWorkflowVersion() {
        return workflowVersion;
    }

    public void setWorkflowVersion(String workflowVersion) {
        this.workflowVersion = workflowVersion;
    }

    /**
     * Forwards an event to the Ambrose server
     * 
     * @param queryId
     * @param event
     */
    public void pushEvent(String queryId, Event<?> event) {
        try {
            service.pushEvent(queryId, event);
        }
        catch (IOException e) {
            LOG.error("Couldn't send event to StatsWriteService!", e);
        }
    }
    
    /**
     * Saves events and DAGNodes for a given workflow
     */
    public void saveEventStack() {
        allEvents.putAll(_eventMap);
        allDagNodes.putAll(service.getDagNodeNameMap(null));
    }
    
    /**
     * Restores events and DAGNodes of all workflows within a script
     * This enables to replay all the workflows when the script finishes
     */
    public void restoreEventStack() {
        _eventMap.putAll(allEvents);
        service.getDagNodeNameMap(null).putAll(allDagNodes);
    }
    
    public void sendDagNodeNameMap(String queryId, Map<String, DAGNode<Job>> nodeIdToDAGNode) {
        try {
            service.sendDagNodeNameMap(queryId, nodeIdToDAGNode);
        }
        catch (IOException e) {
            LOG.error("Couldn't send DAGNode information to server!", e);
        }
    }
    
    public void flushJsonToDisk() {
        try {
            service.flushJsonToDisk();
        }
        catch (IOException e) {
            LOG.warn("Couldn't write json to disk", e);
        }
    }

    public void stopServer() {
        LOG.info("Stopping Ambrose Server...");
        server.stop();
    }

    public void setTotalMRJobs(int totalMRJobs) {
        this.totalMRJobs = totalMRJobs;
    }

    public Set<String> getCompletedJobIds() {
        return completedJobIds;
    }

    public void addCompletedJobIds(String jobID) {
        completedJobIds.add(jobID);
    }
    
}

/*
Copyright 2013, Lorand Bendig

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
package com.twitter.ambrose.hive;

import static com.twitter.ambrose.hive.reporter.AmbroseHiveReporterFactory.getEmbeddedProgressReporter;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;

import com.google.common.collect.Maps;
import com.twitter.ambrose.hive.reporter.EmbeddedAmbroseHiveProgressReporter;
import com.twitter.ambrose.model.DAGNode;
import com.twitter.ambrose.model.Event;
import com.twitter.ambrose.model.Event.WorkflowProgressField;
import com.twitter.ambrose.model.Job;

/**
 * Hook invoked before running a workflow. <br>
 * Constructs DAGNode representation and initializes
 * {@link com.twitter.ambrose.hive.HiveProgressReporter HiveProgressReporter} <br>
 * Called by the main thread
 * 
 * @author Lorand Bendig <lbendig@gmail.com>
 * 
 */
public class AmbroseHivePreHook implements ExecuteWithHookContext {

    private static final Log LOG = LogFactory.getLog(AmbroseHivePreHook.class);

    /** Timeout in seconds for waiting between two workflows */
    private static final String WF_BETWEEN_SLEEP_SECS_PARAM = "ambrose.wf.between.sleep.seconds";
    private static final String SCRIPT_STARTED_PARAM = "ambrose.script.started";

    @Override
    public void run(HookContext hookContext) throws Exception {

        String queryId = AmbroseHiveUtil.getHiveQueryId(hookContext.getConf());
        EmbeddedAmbroseHiveProgressReporter reporter = getEmbeddedProgressReporter();
        HiveDAGTransformer transformer = new HiveDAGTransformer(hookContext);
       
        //conditional tasks may be filtered out by Hive at runtime. We them as
        //'complete'
        Map<String, DAGNode<Job>> nodeIdToDAGNode = reporter.getNodeIdToDAGNode();
        sendFilteredJobsStatus(queryId, reporter, nodeIdToDAGNode);
        if (transformer.getTotalMRJobs() == 0) {
            return;
        }

        waitBetween(hookContext, reporter, queryId);
        
        nodeIdToDAGNode = transformer.getNodeIdToDAGNode();
        reporter.setNodeIdToDAGNode(nodeIdToDAGNode);
        reporter.setTotalMRJobs(transformer.getTotalMRJobs());
        reporter.sendDagNodeNameMap(queryId, nodeIdToDAGNode);

    }

    /**
     * Waiting <tt>ambrose.wf.between.sleep.seconds</tt> before processing the
     * next statement (workflow) in the submitted script
     * 
     * @param hookContext
     * @param reporter
     * @param queryId
     */
    private void waitBetween(HookContext hookContext, EmbeddedAmbroseHiveProgressReporter reporter, String queryId) {

        Configuration conf = hookContext.getConf();
        boolean justStarted = conf.getBoolean(SCRIPT_STARTED_PARAM, true);
        if (justStarted) {
            conf.setBoolean(SCRIPT_STARTED_PARAM, false);
        }
        else {
            // sleeping between workflows
            int sleepTimeMs = conf.getInt(WF_BETWEEN_SLEEP_SECS_PARAM, 10);
            try {

                LOG.info("One workflow complete, sleeping for " + sleepTimeMs
                        + " sec(s) before moving to the next one if exists. Hit ctrl-c to exit.");
                Thread.sleep(sleepTimeMs * 1000L);
                
                //send progressbar reset event
                Map<WorkflowProgressField, String> eventData = Maps.newHashMapWithExpectedSize(1);
                eventData.put(WorkflowProgressField.workflowProgress, "0");
                reporter.pushEvent(queryId, new Event.WorkflowProgressEvent(eventData));
                
                reporter.saveEventStack();
                reporter.reset();
            }
            catch (InterruptedException e) {
                LOG.warn("Sleep interrupted", e);
            }
        }
    }

    private void sendFilteredJobsStatus(String queryId, 
        EmbeddedAmbroseHiveProgressReporter reporter, Map<String, DAGNode<Job>> nodeIdToDAGNode) {
        
        if (nodeIdToDAGNode == null) {
            return;
        }
        
        Map<WorkflowProgressField, String> eventData = 
            new HashMap<Event.WorkflowProgressField, String>(1);

        int skipped = 0;
        for (DAGNode<Job> dagNode : nodeIdToDAGNode.values()) {
            Job job = dagNode.getJob();
            // filtered jobs don't have assigned jobId
            if (job.getId() != null) {
                continue;
            }
            String nodeId = dagNode.getName();
            job.setId(AmbroseHiveUtil.asDisplayId(queryId, "filtered out", nodeId));
            reporter.addJobIdToProgress(nodeId, 100);
            reporter.pushEvent(queryId, new Event.JobFinishedEvent(dagNode));
            skipped++;
        }
        // sleep so that all these events will be visible on GUI before going on
        try {
            Thread.sleep(skipped * 1000L);
        }
        catch (InterruptedException e) {
            LOG.warn("Sleep interrupted", e);
        }

        eventData.put(WorkflowProgressField.workflowProgress,
                Integer.toString(reporter.getOverallProgress()));
        reporter.pushEvent(queryId, new Event.WorkflowProgressEvent(eventData));

    }

}

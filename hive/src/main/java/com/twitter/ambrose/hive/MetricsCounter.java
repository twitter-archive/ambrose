package com.twitter.ambrose.hive;

import java.util.HashMap;
import java.util.Map;

/**
 * Lookup class that constructs Counter names to be retrieved from Hive published
 * statistics. Supports (legacy) Hadoop 0.20.x.x/1.x.x and YARN counter names.
 * 
 * @author Lorand Bendig <lbendig@gmail.com>
 *
 */
public enum MetricsCounter {

    // Task counters
    SLOTS_MILLIS_MAPS(1), 
    SLOTS_MILLIS_REDUCES(1),

    // Filesystem counters
    FILE_BYTES_WRITTEN(2), 
    HDFS_BYTES_WRITTEN(2),

    // Task counters
    MAP_INPUT_RECORDS(3), 
    MAP_OUTPUT_RECORDS(3), 
    SPILLED_RECORDS(3), 
    REDUCE_INPUT_RECORDS(3), 
    REDUCE_OUTPUT_RECORDS(3);

    private static final Map<MetricsCounter, String[]> lookup = new HashMap<MetricsCounter, String[]>();
    static {
        for (MetricsCounter hjc : MetricsCounter.values()) {
            lookup.put(hjc, createLookupKeys(hjc));
        }
    }

    private int type;
    private MetricsCounter(int type) {
        this.type = type;
    }

    private static final String JOB_COUNTER = "org.apache.hadoop.mapred.JobInProgress$Counter";
    private static final String TASK_COUNTER = "org.apache.hadoop.mapred.Task$Counter";
    private static final String FS_COUNTER = "FileSystemCounters";

    private static final String JOB_COUNTER_YARN = "org.apache.hadoop.mapreduce.JobCounter";
    private static final String TASK_COUNTER_YARN = "org.apache.hadoop.mapreduce.TaskCounter";
    private static final String FS_COUNTER_YARN = "org.apache.hadoop.mapreduce.FileSystemCounter";

    public static String[] get(MetricsCounter hjc) {
        return lookup.get(hjc);
    }

    private static String[] createLookupKeys(MetricsCounter hjc) {
        switch (hjc.type) {
        //Job counter (type-1)
        case 1 : return new String[]{
                JOB_COUNTER + "::" + hjc.name(),
                JOB_COUNTER_YARN + "::" + hjc.name()
                };
        //Task counter (type-2)
        case 2 : return new String[]{
                TASK_COUNTER + "::" + hjc.name(),
                TASK_COUNTER_YARN + "::" + hjc.name()
                };
        //Filesystem counter (type-3)
        case 3 : return new String[]{
                FS_COUNTER + "::" + hjc.name(),
                FS_COUNTER_YARN + "::" + hjc.name()
                };
        default : return null;
        }
    }

}

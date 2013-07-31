-- Demo Hive script for use with hive-ambrose,  based on the test case
-- https://svn.apache.org/repos/asf/hive/branches/branch-0.11/ql/src/test/queries/clientpositive/union28.q
-- 
-- To run in mapreduce mode, copy demo folder to hdfs:
-- 
-- cd /path/to/ambrose-hive
-- hadoop fs -put demo .
-- ./bin/hive-ambrose -f ./demo/demo.q
-- 
-- To store jobs and events json data, update HIVE_OPTS before invoking pig-ambrose:
-- 
-- export HIVE_OPTS="\
-- -Dambrose.write.dag.file=jobs.json \
-- -Dambrose.write.events.file=events.json"
-- 
--

SET hivevar:input_path=demo/input;

SET hive.auto.convert.join = true;
SET hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=4;
SET mapred.reduce.tasks=4;

DROP TABLE IF EXISTS src;
CREATE TABLE src (key STRING, value STRING) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '/home/lori/workspace/github/ambrose/hive/src/main/demo/input/kv1.txt' INTO TABLE src;

select * from (
  select key, value from src 
  union all 
  select key, value from 
  (
    select key, value, count(1) from src group by key, value
    union all
    select key, value, count(1) from src group by key, value
  ) subq
) a;

-- Demo Hive script for use with hive-ambrose,  based on the test case
-- https://svn.apache.org/repos/asf/hive/branches/branch-0.11/ql/src/test/queries/clientpositive/union28.q
-- 
-- To run in mapreduce mode, copy demo folder to hdfs:
-- 
-- cd /path/to/ambrose/hive/target/ambrose-hive-$VERSION-bin/ambrose-hive-$VERSION
-- hadoop fs -put demo /
-- ./bin/hive-ambrose -f ./demo/demo.q
-- 
-- To store jobs and events json data, update HIVE_OPTS before invoking pig-ambrose:
-- 
-- export HIVE_OPTS="\
-- -Dambrose.write.dag.file=jobs.json \
-- -Dambrose.write.events.file=events.json"
-- 
--

SET hive.auto.convert.join = true;
SET hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=4;
SET mapred.reduce.tasks=4;

DROP TABLE IF EXISTS ambrose_hive_demo;
CREATE EXTERNAL TABLE ambrose_hive_demo (key STRING, value STRING) 
  ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\001'
  LINES TERMINATED BY '\n'
  STORED AS TEXTFILE
  LOCATION '/demo/input';

select * from (
  select key, value from ambrose_hive_demo 
  union all 
  select key, value from 
  (
    select key, value, count(1) from ambrose_hive_demo group by key, value
    union all
    select key, value, count(1) from ambrose_hive_demo group by key, value
  ) subq
) a;

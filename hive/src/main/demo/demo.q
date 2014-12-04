-- Demo Hive script for use with hive-ambrose
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

SET hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=2;
SET mapred.reduce.tasks=4;

DROP TABLE IF EXISTS ambrose_hive_demo;
CREATE EXTERNAL TABLE ambrose_hive_demo (key STRING, value STRING) 
  ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\001'
  LINES TERMINATED BY '\n'
  STORED AS TEXTFILE
  LOCATION '/demo/input';

DROP VIEW IF EXISTS ambrose_hive_demo_view1;
CREATE VIEW ambrose_hive_demo_view1 as 
  select v1q.key as key, v1q.value as value from (
    select v1q1.key, v1q1.value from ambrose_hive_demo v1q1 group by v1q1.key, v1q1.value 
    union all 
    select v1q2.key, v1q2.value from ambrose_hive_demo v1q2 group by v1q2.key, v1q2.value
  ) v1q;

DROP VIEW IF EXISTS ambrose_hive_demo_view2;
CREATE VIEW ambrose_hive_demo_view2 as 
  select v2q.key as key, v2q.value as value from (
    select v2q1.key, v2q2.value from ambrose_hive_demo v2q1 
      left outer join ambrose_hive_demo v2q2 on (v2q1.key = v2q2.key) 
    union all 
    select v2q2.key, v2q3.value from ambrose_hive_demo v2q2 
      left outer join ambrose_hive_demo v2q3 on (v2q2.key = v2q3.key)
  ) v2q;

select * from (
  select view1.key, view2.value from (
    select view1a.key, view1b.value from ambrose_hive_demo_view1 view1a
      cross join ambrose_hive_demo_view1 view1b on view1a.key = view1b.key) view1
    join
    ambrose_hive_demo_view2 view2 on (view1.key=view2.key)
) q limit 5;

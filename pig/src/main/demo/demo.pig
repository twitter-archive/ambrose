/*

Demo pig script for use with pig-ambrose. Reads some data and does some stuff.

To run in local mode and capture jobs and events json data:

{{{

cd /path/to/ambrose-pig
./bin/pig-ambrose -x local -F -f ./demo/demo.pig

}}}

To run in mapreduce mode, copy demo folder to hdfs:

{{{

cd /path/to/ambrose-pig
hadoop fs -put demo .
./bin/pig-ambrose -x mapreduce -F -f ./demo/demo.pig

}}}

To store jobs and events json data, update PIG_OPTS before invoking pig-ambrose:

{{{

export PIG_OPTS="\
-Dambrose.write.dag.file=jobs.json \
-Dambrose.write.events.file=events.json"

}}}

*/

%default DEMO_DIR 'demo';
%default INPUT_PATH '$DEMO_DIR/input';
%default OUTPUT_PATH '$DEMO_DIR/output';

SET job.name 'Ambrose Pig demo script';

-- load data
user = LOAD '$INPUT_PATH/users.tsv' AS (user_id: long, name: chararray);
friend = LOAD '$INPUT_PATH/friends.tsv' AS (user_id: long, friend_id: long);
enemy = LOAD '$INPUT_PATH/enemies.tsv' AS (user_id: long, enemy_id: long);
tweet = LOAD '$INPUT_PATH/tweets.tsv' AS (tweet_id: long, user_id: long, timestamp: long, reply_tweet_id: long, text: chararray);

-- group friend, enemy links by source user id
user_friends = FOREACH (GROUP friend BY user_id) GENERATE
  group AS user_id, friend.(friend_id) AS friends;
user_enemies = FOREACH (GROUP enemy BY user_id) GENERATE
  group AS user_id, enemy.(enemy_id) AS enemies;

-- join user data
user = FOREACH (JOIN
  user BY user_id LEFT OUTER, user_friends BY user_id
) GENERATE user::user_id AS user_id, user::name AS name,
  user_friends::friends AS friends;
user = FOREACH (JOIN
  user BY user_id LEFT OUTER, user_enemies BY user_id
) GENERATE user::user_id AS user_id, user::name AS name,
  user::friends AS friends,
  user_enemies::enemies AS enemies;

-- find user friend, enemy counts
user_counts = FOREACH user GENERATE
  user_id,
  (friends IS NULL ? 0 : COUNT(friends)) AS num_friends,
  (enemies IS NULL ? 0 : COUNT(enemies)) AS num_enemies;

-- calc histograms of num {friends, enemies} vs num users
friends_users_histogram = FOREACH (GROUP user_counts BY num_friends) GENERATE
  group AS friends, COUNT(user_counts) AS users;
enemies_users_histogram = FOREACH (GROUP user_counts BY num_enemies) GENERATE
  group AS enemies, COUNT(user_counts) AS users;

-- find users who are their own friend
narcissists = FOREACH user GENERATE user_id, name, FLATTEN(friends);
narcissists = FILTER narcissists BY user_id == friend_id;
narcissists = FOREACH narcissists GENERATE user_id, name;

-- find users who are their own enemy
masochists = FOREACH user GENERATE user_id, name, FLATTEN(enemies);
masochists = FILTER masochists BY user_id == enemy_id;
masochists = FOREACH masochists GENERATE user_id, name;

-- find the enemies of enemies of users (new user recommendation strategy!!)
user_enemy = FOREACH user GENERATE user_id, name, FLATTEN(enemies) AS (enemy_id);
user_enemy2 = FOREACH user_enemy GENERATE user_id, enemy_id;
user_enemy_enemy = FOREACH (JOIN
  user_enemy BY enemy_id, user_enemy2 BY user_id
) GENERATE user_enemy::user_id AS user_id, user_enemy::name AS name,
  user_enemy2::enemy_id AS enemy_enemy_id;
frienemies = FOREACH (GROUP user_enemy_enemy BY user_id) {
  name = LIMIT user_enemy_enemy.name 1;
  frienemies = DISTINCT user_enemy_enemy.enemy_enemy_id;
  GENERATE group AS user_id, FLATTEN(name), frienemies AS frienemies;
}

-- tokenize tweets
tokens = FOREACH tweet GENERATE user_id, TOKENIZE(text) AS tokens;
user_token = FOREACH tokens GENERATE user_id, FLATTEN(tokens) AS (token);

-- find @mentions
user_mention_raw = FILTER user_token BY
  token IS NOT NULL AND
  SIZE(token) > 0L AND
  SUBSTRING(token, 0, 1) == '@';
user_mention_name = FOREACH user_mention_raw GENERATE
  user_id, SUBSTRING(token, 1, (int) SIZE(token)) AS mention_name;

-- join mentions with user data to recover ids
user_name = FOREACH user GENERATE user_id, name;
user_mention = FOREACH (JOIN
  user_mention_name BY mention_name, user_name BY name
) GENERATE user_mention_name::user_id AS user_id, user_name::user_id AS mention_id;

-- group mentions by user
user_mentions = FOREACH (GROUP user_mention BY user_id) GENERATE
  group AS user_id, user_mention.(mention_id) AS mentions;

-- join all user data
user = FOREACH (JOIN
  user BY user_id LEFT OUTER, user_mentions BY user_id
) GENERATE user::user_id AS user_id, user::name AS name,
  user::friends AS friends, user::enemies AS enemies,
  user_mentions::mentions AS mentions;

-- store data
rmf $OUTPUT_PATH
STORE friends_users_histogram INTO '$OUTPUT_PATH/friends_users_hist';
STORE enemies_users_histogram INTO '$OUTPUT_PATH/enemies_users_hist';
STORE narcissists INTO '$OUTPUT_PATH/narcissists';
STORE masochists INTO '$OUTPUT_PATH/masochists';
STORE frienemies INTO '$OUTPUT_PATH/frenemies';
STORE user INTO '$OUTPUT_PATH/users';

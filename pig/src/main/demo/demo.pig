/*

Demo pig script for use with pig-ambrose. Reads some data and does some stuff.

*/

%default BASEDIR `pwd`;
%default INPUT_PATH 'file://$BASEDIR/demo/data.tsv';
%default OUTPUT_PATH 'file://$BASEDIR/demo/output';

user = LOAD '$INPUT_PATH' AS (
  user_id: long, name: chararray, friends: {(user_id: long)}, enemies: {(user_id: long)}
);

user_counts = FOREACH user GENERATE
  user_id, COUNT(friends) AS num_friends, COUNT(enemies) AS num_enemies;

friends_users_histogram = FOREACH (GROUP user_counts BY num_friends) GENERATE
  group AS friends, COUNT(user_counts) AS users;

enemies_users_histogram = FOREACH (GROUP user_counts BY num_enemies) GENERATE
  group AS enemies, COUNT(user_counts) AS users;

narcissists = FOREACH user GENERATE user_id, name, FLATTEN(friends) AS (friend_id);
narcissists = FILTER narcissists BY user_id == friend_id;
narcissists = FOREACH narcissists GENERATE user_id, name;

masochists = FOREACH user GENERATE user_id, name, FLATTEN(enemies) AS (enemy_id);
masochists = FILTER masochists BY user_id == enemy_id;
masochists = FOREACH masochists GENERATE user_id, name;

user_enemy = FOREACH user GENERATE user_id, name, FLATTEN(enemies) AS (enemy_id);
user_enemy2 = FOREACH user_enemy GENERATE user_id, enemy_id;
user_enemy_enemy = FOREACH (JOIN
  user_enemy BY enemy_id, user_enemy2 BY user_id
) GENERATE user_enemy::user_id AS user_id, user_enemy::name AS name,
  user_enemy2::enemy_id AS enemy_enemy_id;
user_enemies_of_enemies = FOREACH (GROUP user_enemy_enemy BY user_id) {
  name = LIMIT user_enemy_enemy.name 1;
  enemies_of_enemies = DISTINCT user_enemy_enemy.enemy_enemy_id;
  GENERATE group AS user_id, FLATTEN(name), enemies_of_enemies AS enemies_of_enemies;
}

rmf $OUTPUT_PATH
STORE friends_users_histogram INTO '$OUTPUT_PATH/friends_users_hist';
STORE enemies_users_histogram INTO '$OUTPUT_PATH/enemies_users_hist';
STORE narcissists INTO '$OUTPUT_PATH/narcissists';
STORE masochists INTO '$OUTPUT_PATH/masochists';
STORE user_enemies_of_enemies INTO '$OUTPUT_PATH/frenemies';

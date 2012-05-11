# Ambrose Pig Support

## Implementation

Ambrose integrates with Pig via Pig's `PigProgressNotificationListener` interface. The `ambrose-pig`
script launches Pig with the Ambrose implementation of PPNL. This implementation starts an embedded
[Jetty](http://jetty.codehaus.org/jetty/) server that exposes job runtime information to the Ambrose web UI.

## Known issues

* Ambrose currently requires Apache Pig's `0.11.0-SNAPSHOT` build, which is not a production release.
* Pig scripts with `exec` statements in them are not currently supported.

## Pig patches

The Ambrose Pig integration requires a number of patches that are committed on the Pig trunk and
scheduled for release in Pig 0.11.0. Hence, the Ambrose distribution includes a Pig 0.11.0-SNAPSHOT
build. Note that running the `pig-ambrose` script will result in the script being executed with
the Pig 0.11.0-SNAPSHOT runtime.

Running Ambrose with a released version of Pig < 0.11.0 should be possible by applying these patches
to the release:

* [PIG-2660](https://issues.apache.org/jira/browse/PIG-2660) - PPNL should get notified of plan before it gets executed (ready for commit)
* [PIG-2663](https://issues.apache.org/jira/browse/PIG-2663) - Expose helpful ScriptState methods
* [PIG-2664](https://issues.apache.org/jira/browse/PIG-2664) - Allow PPNL impls to get more job info during the run
* [PIG-2525](https://issues.apache.org/jira/browse/PIG-2525) - Support pluggable PigProgressNotifcationListeners on the command line
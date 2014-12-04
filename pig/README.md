# Ambrose Pig Support

## Implementation

Ambrose integrates with Pig 0.11.0+ via Pig's `PigProgressNotificationListener` (PPNL)
interface. The `./bin/pig-ambrose` script launches Pig with the
[`EmbeddedAmbrosePigProgressNotificationListener`](https://github.com/twitter/ambrose/blob/master/pig/src/main/java/com/twitter/ambrose/pig/EmbeddedAmbrosePigProgressNotificationListener.java). This
PPNL records Pig workflow state in memory, and starts an embedded
[Jetty](http://www.eclipse.org/jetty/) web server that hosts the Ambrose web application.

## Known issues

* Pig scripts which include `exec` statements are not currently supported.

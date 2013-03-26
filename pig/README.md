# Ambrose Pig Support

## Implementation

Ambrose integrates with Pig 0.11.0+ via Pig's `PigProgressNotificationListener` (PPNL)
interface. The `./bin/pig-ambrose` script launches Pig with the Ambrose implementation of PPNL. This
implementation starts an embedded [Jetty](http://jetty.codehaus.org/jetty/) server that exposes job
runtime information to the Ambrose web UI.

## Known issues

* Pig scripts which include `exec` statements are not currently supported.

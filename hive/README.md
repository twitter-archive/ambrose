# Ambrose Hive Support

## Implementation

Ambrose integrates with Hive 0.11.0+ via Hive's Hook interface.
The `$AMBROSE_HOME/hive/src/main/scripts/hive-ambrose` script launches Hive with the Ambrose implementation of Hive hooks. This
implementation starts an embedded [Jetty](http://jetty.codehaus.org/jetty/) server that exposes job
runtime information to the Ambrose web UI.

Ambrose-Hive takes all the (query) statements (aka. workflows) in a Hive script and visualizes them
one by one. After the last workflow has finished, all the workflows in that script can be replayed.


## Quickstart

To get started with Ambrose, first clone and build the Ambrose Github repository.
https://github.com/twitter/ambrose


To run Ambrose with an actual Hive script, you'll need to build the Ambrose Hive distribution:

```
mvn package
```

You can then run the following commands to execute `path/to/my/hive.q` with an Ambrose app server
embedded within the Hive client:


```
cd /path/to/ambrose/hive/target/ambrose-hive-$VERSION-bin/ambrose-hive-$VERSION
./bin/hive-ambrose -f path/to/my/hive.q
```

Note that this command delegates to the `hive` script present in your local installation of Hive, so
make sure `$HIVE_HOME/bin` is in your path. Now, browse to
[http://localhost:8080/workflow.html](http://localhost:8080/workflow.html) to see the
progress of your script using the Ambrose UI. To override the default port, export `AMBROSE_PORT`
before invoking `hive-ambrose`:

```
export AMBROSE_PORT=4567
```

Other parameters that can be overridden:

AMBROSE_WF_BETWEEN_TIMEOUT : Number of seconds to wait before processing the next workflow in a script (default : 10 sec)
AMBROSE_TIMEOUT : Number of seconds to keep the VM running after the script is complete (default: 10 min)


## Notes / Known issues

* When running on YARN make sure that AMBROSE_PORT and YARN's ShuffleHandler won't listen on the same port (both uses 8080 by default)
* More intuitive alias naming
* Page reload is needed between two workflows

## Authors
* [Lorand Bendig](https://github.com/lbendig) ([@lorandbendig](https://twitter.com/lorandbendig))
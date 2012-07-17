# Ambrose [![Build Status](https://secure.travis-ci.org/twitter/ambrose.png)](http://travis-ci.org/twitter/ambrose)

Twitter Ambrose is a platform for visualization and real-time monitoring of MapReduce data workflows.
It presents a global view of all the map-reduce jobs derived from your workflow after planning and
optimization. As jobs are submitted for execution on your Hadoop cluster, Ambrose updates its
visualization to reflect the latest job status, polled from your process.

Ambrose provides the following in a web UI:

* A chord diagram to visualize job dependencies and current state
* A table view of all the associated jobs, along with their current state
* A highlight view of the currently running jobs
* An overall script progress bar

Ambrose is built using the following front-end technologies:

* [D3.js](http://d3js.org) - For chord diagram visualization
* [Bootstrap](http://twitter.github.com/bootstrap/) - For layout and CSS support

Ambrose is designed to support any Hadoop workflow runtime, but current support is limited to
[Apache Pig](http://pig.apache.org/).

Follow [@Ambrose](https://twitter.com/ambrose) on Twitter to stay in touch!

## Supported runtimes

* [Pig](http://pig.apache.org/) - See [pig/README.md](https://github.com/twitter/ambrose/blob/master/pig/README.md)
* [Cascading](http://www.cascading.org/) - future work
* [Scalding](https://github.com/twitter/scalding) - future work
* [Cascalog](https://github.com/nathanmarz/cascalog) - future work
* [Hive](http://hive.apache.org/) - future work

## Examples

Below is a screenshot of the Ambrose UI. Each arc segment on the circle represents a map-reduce job.
Dependencies between jobs are represented by chords which connect job arc segments.
Grey jobs have not yet run, bright green jobs are running and light green jobs are completed.
When the mouse hovers over a job, its arc and input dependencies are highlighted blue. Clicking on
the job will select it, updating the contents of the table to the right of the diagram with
information about the selected job.

Note that Each job arc is bisected; Chords on one half of the arc connect to predecessor jobs while
chords on the other half connect to successor jobs. For example, in the diagram below jobs 1 and 3
have no predecessors while jobs 16, 20, 21, and 22 have no successors (their outputs represent the
final result of this workflow).

Note that the chord diagram shown is our first pass at visualizing the workflow, and there's room
for improvement. We'd like to support other visualizations as well, like a graph of the workflow DAG.
If you develop an improved visualization, be sure to send us a pull request!

![Ambrose UI screenshot](https://github.com/twitter/ambrose/raw/master/docs/img/ambrose-ss1.png)

## Quickstart

To get started with Ambrose, first clone the Ambrose Github repository:

```
git clone https://github.com/twitter/ambrose.git
cd ambrose
```

Next, you can try running the Ambrose demo on your local machine. The demo
starts a local web server which serves the front-end client resources and sample
data. Start the demo with the following command and then browse to
[http://localhost:8080/index.html?localdata=small](http://localhost:8080/index.html?localdata=small):

```
./bin/ambrose-demo
```

To run Ambrose with an actual Pig script, you'll need to build the Ambrose Pig
distribution and untar it:

```
mvn package
tar zxvf pig/target/ambrose-pig-$VERSION-bin.tar.gz
```

You can then run the following commands to execute `path/to/my/script.pig` with an Ambrose app server
embedded within the Pig client:

```
cd ambrose-pig-$VERSION
./bin/pig-ambrose -f path/to/my/script.pig
```

Note that this command delegates to the `pig` script present in your local
installation of Pig, so make sure `$PIG_HOME/bin` is in your path. Now, browse
to [http://localhost:8080/](http://localhost:8080/) to see the progress of your
script using the Ambrose UI. To override the default port, export `AMBROSE_PORT`
before invoking `pig-ambrose`:

```
export AMBROSE_PORT=4567
```

## Maven repository

An initial release will be pushed to Maven shortly.

## How to contribute

Bug fixes, features, and documentation improvements are welcome! Please fork the project and send us
a pull request on Github. You can [submit issues on Github](https://github.com/twitter/ambrose/issues)
as well.

Here are some high-level goals we'd love to see contributions for:

* Improve the front-end client
* Add other visualization options, like a DAG view
* Create a new back-end for a different runtime environment
* Create a standalone Ambrose server that's not embedded in the workflow client

## Versioning

For transparency and insight into our release cycle, releases will be numbered with the follow format:

`<major>.<minor>.<patch>`

And constructed with the following guidelines:

* Breaking backwards compatibility bumps the major
* New additions without breaking backwards compatibility bumps the minor
* Bug fixes and misc changes bump the patch

For more information on semantic versioning, please visit http://semver.org/.

## Authors

* Bill Graham ([@billgraham](https://twitter.com/billgraham))
* Andy Schlaikjer ([@sagemintblue](https://twitter.com/sagemintblue))
* Nicolas Belmonte ([@philogb](https://twitter.com/philogb))

## License

Copyright 2012 Twitter, Inc.

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0

# Ambrose [![Build Status](https://secure.travis-ci.org/twitter/ambrose.png)](http://travis-ci.org/twitter/ambrose)

Twitter Ambrose is a platform for visualization and real-time monitoring of Map Reduce data workflows.
It presents a global view of all the map-reduce jobs derived from your workflow after planning and
optimization. As jobs are submitted for execution on your Hadoop cluster, Ambrose updates its
visualization to reflect the latest job status, polled from your process.

Ambrose provides the following in a web UI:

* A chord diagram to visualize job dependencies and current state
* A table view of all the associated jobs, along with their current state
* A highlight view of the currently running jobs
* An overall script progress bar

Ambrose is built using the following front-end technologies:

* [d3.js](http://d3js.org) - For chord diagram visualization
* [Bootstrap](http://twitter.github.com/bootstrap/) - For layout and CSS support

Ambrose is designed to support any Hadoop workflow runtime, but current support is limited to
[Apache Pig](http://pig.apache.com/).

## Supported runtimes

* [Pig](http://pig.apache.com/) - See [pig/README.md](ambrose/blob/master/pig/README.md)
* [Cascading](http://www.cascading.org/) - future work
* [Scalding](https://github.com/twitter/scalding) - future work

## Examples

Below is a screenshot of the Ambrose UI. Each arc segment on the circle represents a map-reduce job.
Dependencies between jobs are represented by chords which connect job arc segments.
Grey jobs have not yet run, bright green jobs are running and light green jobs are completed.

Note that Each job arc is bisected; Chords on one half of the arc connect to predecessor jobs while
chords on the other half connect to successor jobs. For example, in the diagram below Jobs 10 and 13
have no predecessors and Jobs 8 and 18 are the final jobs in the Pig workflow.

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

Next, you can try running the Ambrose demo on your local machine. The `ambrose-demo` script starts a
local instance of the Ambrose app server with sample data. Start the demo Abrose server with the
following command and then brose to
[http://localhost:8080/index.html?localdata=small](http://localhost:8080/index.html?localdata=small):

```
./bin/ambrose-demo
```

Finally, you can run Ambrose with an actual Pig script. To do so, you'll need to build the
Ambrose distribution and untar it:

```
./bin/ambrose-package
tar zxvf ambrose-0.1-SNAPSHOT.tar.gz
```

You can then run the following commands to execute `path/to/my/script.pig` with an Ambrose app server
embedded in the Pig client:

```
cd ambrose-0.1-SNAPSHOT
./bin/pig-ambrose -f path/to/my/script.pig
```

Now, browse to [http://localhost:8080/](http://localhost:8080/) to see the progress of you script
using the Ambrose UI. To override the default port, export `AMBROSE_PORT` before invoking `pig-ambrose`:

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

## License

Copyright 2012 Twitter, Inc.

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0

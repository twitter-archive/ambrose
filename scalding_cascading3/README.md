# Ambrose Scalding Support on Cascading 3

## Usage

To enable the embedded Ambrose server in your Scalding job, add the
[`AmbroseAdapter`](https://github.com/twitter/ambrose/blob/master/scalding_cascading3/src/main/scala/com/twitter/ambrose/scalding_cascading3/AmbroseAdapter.scala)
trait to it. Then, while the job is running, browse to http://localhost:8080/. See
[`EmbeddedAmbroseCascadingNotifier`](https://github.com/twitter/ambrose/blob/master/cascading3/src/main/java/com/twitter/ambrose/cascading3/EmbeddedAmbroseCascadingNotifier.java)
for details on configuring the port, etc.

## Authors

* [twdima](https://github.com/twdima) ([@dimatkach69](https://twitter.com/dimatkach69))

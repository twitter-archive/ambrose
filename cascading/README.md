# Ambrose Cascading Support

## Implementation

Ambrose integrates with Cascading via Cascading's `FlowListener` and `FlowStepListener`
interfaces. The
[`AmbroseCascadingNotifier`](https://github.com/twitter/ambrose/blob/master/cascading/src/main/java/com/twitter/ambrose/cascading/AmbroseCascadingNotifier.java)
implements both of these interfaces, and passes Cascading flow events on to an Ambrose
[`StatsWriteService`](https://github.com/twitter/ambrose/blob/master/common/src/main/java/com/twitter/ambrose/service/StatsWriteService.java). For
more information on Cascading see [Cascading Getting
Started]([http://www.cascading.org/documentation/).

The
[`EmbeddedAmbroseCascadingNotifier`](https://github.com/twitter/ambrose/blob/master/cascading/src/main/java/com/twitter/ambrose/cascading/EmbeddedAmbroseCascadingNotifier.java),
which extends `AmbroseCascadingNotifier`, records flow state in memory, and starts an embedded
[Jetty](http://www.eclipse.org/jetty/) web server that hosts the Ambrose web application.

To use the `EmbeddedAmbroseCascadingNotifier` in your Cascading program, add the following code at
the end of Cascading main:

```
// creates the embedded cascading notifier before tfidfFlow.complete();
EmbeddedAmbroseCascadingNotifier server = new EmbeddedAmbroseCascadingNotifier();
```

Then, add the listeners to your Flow:

```
flow.addListener(server);
flow.addStepListener(server);
flow.complete();
```

When your Cascading program executes, the embedded Jetty web server will (by default) bind to
localhost port 8080, allowing you to browse to http://localhost:8080/ to see the Ambrose web
application and its visualization of workflow state.

## Authors

* [Ahmed Mohsen](https://github.com/Ahmed--Mohsen) ([@Ahmed__Mohsen](https://twitter.com/Ahmed__Mohsen))
* [Ahmed Eshra](https://github.com/engeshra) ([@engeshra](https://twitter.com/engeshra))

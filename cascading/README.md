# Ambrose Cascading Support

## Implementation 

Cascading integrates with Ambrose via the ```AmbroseCascadingNotificationListener``` class. Cascading starts an
embedded [Jetty](http://jetty.codehaus.org/jetty/) server that exposes job information to the Ambrose Web server.
For more information on Cascading see [Cascading Getting Started]([http://www.cascading.org/documentation/).

To run Ambrose with a Cascading program add the following code at the end of Cascading main:

```
// creates the embedded cascading listener before tfidfFlow.complete();
EmbeddedAmbroseCascadingProgressNotificationListener server = new EmbeddedAmbroseCascadingProgressNotificationListener();
```

Then, add the listeners to the Flow:

```
tfidfFlow.addListener(server);
tfidfFlow.addStepListener(server);
tfidfFlow.complete();
```

Note: ```tfidfFlow``` is the Flow of the Cascading example
[cascading for the impatient part 5](http://www.cascading.org/2012/07/31/cascading-for-the-impatient-part-5/).

## Authors
* [Ahmed Mohsen](https://github.com/Ahmed--Mohsen) ([@Ahmed__Mohsen](https://twitter.com/Ahmed__Mohsen))
* Ahmed Eshra ([@engeshra](https://twitter.com/engeshra))

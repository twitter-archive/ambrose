# Ambrose Cascading Support

## Implementation 

Cascading integrates with Ambrose via "AmbroseCascadingNotificationListener" class. Cascading starts an 
embedded [Jetty](http://jetty.codehaus.org/jetty/) server that exposes job information to Ambrose Web server.

To run Ambrose with an actual Cascading program (Cascading Getting Started)[http://www.cascading.org/documentation/]


In Cascading main, you have to add 2 lines of code at the end of the main:

create the embedded cascading listener :

```
// before tfidfFlow.complete();

EmbeddedAmbroseCascadingProgressNotificationListener server = new EmbeddedAmbroseCascadingProgressNotificationListener();

```

Then, add the listeners to Flow :

```
tfidfFlow.addListener(server);
tfidfFlow.addStepListener(server);
tfidfFlow.complete();
```
Note: tfidfFlow is the Flow of Cascading example: (impatient part%) [http://www.cascading.org/2012/07/31/cascading-for-the-impatient-part-5/]

## Authors
* Ahmed Mohsen ([@Ahmed--Mohsen](https://twitter.com/Ahmed__Mohsen))
* Ahmed Eshra ([@engeshra](https://twitter.com/engeshra))



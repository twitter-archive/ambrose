/*
Copyright 2013 Twitter, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/


package com.twitter.ambrose.cascading;

import cascading.flow.Flow;
import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowProcess;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.*;

/** Select and configure the type of listener to communicate cascading job progress to ambrose.
  * The two "out-of-the-box" possibilities are:
  * - Embedded server  (@see com.twitter.ambrose.cascading.EmbeddedAmbroseCascadingNotifier) or
  * - HRaven connector (@see tter.ambrose.cascading.HRavenAmbroseCascadingNotifier)
  *
  * The decision of which one to use is made as follows:
  * - If AMBROSE_NOTIFIER_CLASS environment variable is set, its value is used, otherwise
  * - A property ambrose.notifier.class is checked (see below about property definitions).
  * - If the value is not defined (or is empty), the default notifier is selected to be HRaven connector
  *   if hraven jar is present in classpath, and embedded server otherwise.
  * 
  * Possible values for the property/env settings are (case insensitive, except for actual class names)::
  * - Empty: see above for the description of how default notifier is selected
  * - None:  no notifier. Ambrose will be disabled
  * - Embedded: Use embedded ambrose service
  * - HRaven: Use HRaven connector
  * - some.other.ClassName: a fully qualified class name of a custom notifier class.
  * Must be a subclass of AmbroseCascadingNotifier, and have a public constructor, taking
  * com.twitter.ambrose.cascading.Configuration as a parameter.
  *
  * The properties are looked up in the classpath: first in /config/ambrose-cascading.properties, 
  * and then in /ambrose-cascading.properties, the latter taking precedence for properties defined 
  * in both places. Additionally, if a system property ambrose.properties and/or AMBROSE_PROPERTIES
  * environment variable are set to name(s) of exiting file(s), those are also loade.
  * Finally, AMBROSE_PROPERTIES_OVERRIDE can be set to a semi-colon separated string of overrides:
  * "foo.bar=baz;foo.baz=bar;" etc. (System properties are a better way to do this, but hadoop
  * command isn't passing them into the app from comman line
  *
  * All properties will be copied into the job configuration object, passed to the notifier. 
  * One important property that must be set if using HRaven connector is batch.desc, which is the 
  * application id, required by HRaven. Cascading does not set it the same way Pig does, so, it
  * must be set explicitly in the properties. Set it to a descriptive name of the job being run.
  */

public class AmbroseCascadingNotifierFactory {

  private static Logger LOG = LoggerFactory.getLogger(AmbroseCascadingNotifierFactory.class);

  private static InputStream openResource(String name) {
    if(name.startsWith("cp:")) {
      return AmbroseCascadingNotifierFactory.class.getResourceAsStream(name.substring(3)
            + "/" + "ambrose-cascading.properties");
    }

    try {
      return new FileInputStream(name);
    }
    catch(IOException e) {
      LOG.warn("Failed to open property file " + name, e);
    }

    return null;
  }

  private static List<String> candidateList(String ... candidates) {
    List<String> cs = new LinkedList();
    for(String s : candidates) {
      if(s != null && s.length() != 0) cs.add(s);
    }
    return cs;
  }

  private static Properties readProperties() throws IOException {
    Properties props = new Properties();
    List<String> candidates = candidateList("cp:/config", "cp:", 
           System.getProperty("ambrose.properties"), System.getenv("AMBROSE_PROPERTIES"));

    for(String path : candidates) {
      InputStream is = openResource(path);
      if(is == null) { continue; }
      LOG.info("Reading ambrose connector properties from " + path);
      try {
        props.load(new InputStreamReader(is));
      }
      finally {
        is.close();
      }
    }

    String overrides = System.getenv("AMBROSE_PROPERTIES_OVERRIDE");
    if(overrides != null) {
      for(StringTokenizer st = new StringTokenizer(overrides, ";"); st.hasMoreElements();) {
        String kv[] = st.nextToken().trim().split("=");
        if(kv.length == 2) props.put(kv[0], kv[1]);
      }
    }

    return props;
  }

  public static AmbroseCascadingNotifier createNotifier(Flow flow) {
    String clazz = System.getenv("AMBROSE_NOTIFIER_CLASS");
    try {
      Properties props = readProperties();
      if(clazz == null) { clazz = props.getProperty("ambrose.notifer.class"); }

      if(clazz == null || clazz.length() == 0) {
        try {
          Class.forName("com.twitter.hraven.datasource.FlowQueueService");
          clazz = "hraven";
          LOG.info("Using default HRaven connector");
        }
        catch(Exception e) {
          LOG.info("HRaven is not available. Will use embedded notifier.");
          clazz="embedded";
        }
      }

      if("none".equalsIgnoreCase(clazz)) { return null; }
      if("embedded".equalsIgnoreCase(clazz)) {  return new EmbeddedAmbroseCascadingNotifier(); }
      if("hraven".equalsIgnoreCase(clazz)) {
        clazz = "com.twitter.ambrose.cascading.HRavenAmbroseCascadingNotifier";
      }

      FlowProcess fp = flow.getFlowProcess();
      Configuration jobConf =  (fp instanceof HadoopFlowProcess)  ?
          ((HadoopFlowProcess) fp).getJobConf() : new Configuration();

      for(Map.Entry prop : props.entrySet()) {
        jobConf.set(prop.getKey().toString(), prop.getValue().toString());
      }

      return (AmbroseCascadingNotifier) Class.forName(clazz)
          .getConstructor(Configuration.class)
          .newInstance(jobConf);
    }
    catch (Exception e) {
      LOG.error("Failed to create ambrose notifier of type " + clazz + "."
          + " Ambrose will be disabled.", e);
    }
    return null;
  }
}

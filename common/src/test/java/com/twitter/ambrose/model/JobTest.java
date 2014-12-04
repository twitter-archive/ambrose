package com.twitter.ambrose.model;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import org.junit.Test;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.Maps;
import com.twitter.ambrose.util.JSONUtil;

/**
 * Unit tests for {@link com.twitter.ambrose.model.JobTest}.
 */
public class JobTest {

  private void testRoundTrip(Job expected) throws IOException {
    String asJson = expected.toJson();
    Job asJobAgain = Job.fromJson(asJson);
    assertEquals(expected, asJobAgain);
  }

  @Test
  public void testRoundTrip() throws IOException {
    Properties properties = new Properties();
    properties.setProperty("someprop", "propvalue");
    Map<String, Number> metrics = Maps.newHashMap();
    metrics.put("somemetric", 6);
    Job job = new Job("scope-123", properties, metrics);
    testRoundTrip(job);
  }

  @Test
  public void testPolymorphism() throws IOException {
    Job job;

    job = new ExtendedJob1();
    job.setId("extendedjob-1");
    ((ExtendedJob1)job).setAliases(new String[]{"a1", "a2"});
    testRoundTrip(job);

    job = new ExtendedJob2();
    job.setId("extendedjob-2");
    ((ExtendedJob2)job).setFeatures(new String[]{"f1", "f2"});
    testRoundTrip(job);

    // check if you can still deserialize ExtendedJob1
    job = new ExtendedJob1();
    job.setId("extendedjob-3");
    ((ExtendedJob1)job).setAliases(new String[]{"a1", "a2"});
    testRoundTrip(job);
  }
}

@JsonTypeName("extendedjob1")
class ExtendedJob1 extends Job {
  String [] aliases;

  public ExtendedJob1() {
    super();
  }

  @JsonCreator
  public ExtendedJob1(@JsonProperty("aliases") String[] aliases) {
    this.aliases = aliases;
  }

  public void setAliases(String[] aliases) {
    this.aliases = aliases;
  }

  public String[] getAliases() {
    return aliases;
  }
}

@JsonTypeName("extendedjob2")
class ExtendedJob2 extends Job {
  String [] features;

  public ExtendedJob2() {
    super();
  }

  @JsonCreator
  public ExtendedJob2(@JsonProperty("features") String[] features) {
    this.features = features;
  }

  public void setFeatures(String[] features) {
    this.features = features;
  }

  public String[] getFeatures() {
    return features;
  }
}

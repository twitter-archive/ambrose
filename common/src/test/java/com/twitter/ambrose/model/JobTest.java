package com.twitter.ambrose.model;

import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.google.common.collect.Maps;

import static org.junit.Assert.assertEquals;

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
}

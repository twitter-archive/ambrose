package com.twitter.ambrose.model;

import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Unit tests for {@link com.twitter.ambrose.model.JobTest}.
 */
public class JobTest {

  private void testRoundTrip(Job expected) throws IOException {
    String asJson = expected.toJson();
    Job asJobAgain = Job.fromJson(asJson);
    ModelTestUtils.assertJobEquals(expected, asJobAgain);
  }

  @Test
  public void testRoundTrip() throws IOException {
    Map<String, Number> metrics = new HashMap<String, Number>();
    metrics.put("somemetric", 6);
    Properties properties = new Properties();
    properties.setProperty("someprop", "propvalue");
    Job job = new Job("scope-123", metrics, properties);

    testRoundTrip(job);
  }
}

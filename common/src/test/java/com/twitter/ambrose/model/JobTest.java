package com.twitter.ambrose.model;

import com.fasterxml.jackson.core.type.TypeReference;
import com.twitter.ambrose.util.JSONUtil;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Unit tests for {@link com.twitter.ambrose.model.JobTest}.
 */
public class JobTest {
  private String toJson(Job job) throws IOException {
    return JSONUtil.toJson(job);
  }

  private Job toJob(String json) throws IOException {
    return JSONUtil.toObject(json, new TypeReference<Job>() { });
  }

  private void testRoundTrip(Job expected) throws IOException {
    String asJson = toJson(expected);
    Job asJobAgain = toJob(asJson);
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

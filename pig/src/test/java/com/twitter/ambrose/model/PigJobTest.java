package com.twitter.ambrose.model;

import com.fasterxml.jackson.core.type.TypeReference;
import com.twitter.ambrose.pig.PigJob;
import com.twitter.ambrose.util.JSONUtil;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Unit tests for {@link com.twitter.ambrose.model.PigJobTest}.
 */
public class PigJobTest {
  private String toJson(Job job) throws IOException {
    return JSONUtil.toJson(job);
  }

  private PigJob toJob(String json) throws IOException {
    return JSONUtil.toObject(json, new TypeReference<PigJob>() { });
  }

  private void testRoundTrip(PigJob expected) throws IOException {
    String asJson = toJson(expected);
    System.out.println("asJson: " + asJson);
    PigJob asJobAgain = toJob(asJson);
    assertJobEquals(expected, asJobAgain);
  }

  public static void assertJobEquals(PigJob expected, PigJob found) {
    assertEquals(expected.getId(), found.getId());
    assertArrayEquals(expected.getAliases(), found.getAliases());
    assertArrayEquals(expected.getFeatures(), found.getFeatures());
    assertEquals(expected.getMetrics(), found.getMetrics());
    assertEquals(expected.getConfiguration(), found.getConfiguration());
  }

  @Test
  public void testRoundTrip() throws IOException {
    Map<String, Number> metrics = new HashMap<String, Number>();
    metrics.put("somemetric", 6);
    Properties properties = new Properties();
    properties.setProperty("someprop", "propvalue");
    String[] aliases = new String[] { "alias1" };
    String[] features = new String[] { "feature1" };
    PigJob job = new PigJob(aliases, features);

    testRoundTrip(job);
  }
}

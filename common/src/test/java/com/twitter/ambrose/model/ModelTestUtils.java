package com.twitter.ambrose.model;

import static org.junit.Assert.assertEquals;

public class ModelTestUtils {

  public static void assertJobEquals(Job expected, Job found) {
    assertEquals(expected.getId(), found.getId());
    assertEquals(expected.getMetrics(), found.getMetrics());
    assertEquals(expected.getConfiguration(), found.getConfiguration());
  }
}

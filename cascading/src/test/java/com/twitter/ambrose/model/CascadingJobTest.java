/*
Copyright 2014 Twitter, Inc.

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

package com.twitter.ambrose.model;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.junit.Before;
import org.junit.Test;

import com.twitter.ambrose.model.Job;
import com.twitter.ambrose.cascading.CascadingJob;

public class CascadingJobTest {
  CascadingJob cascadingJob;

  @Before
  public void setUp() throws Exception {
    Properties properties = new Properties();
    properties.setProperty("someprop", "propvalue");
    String[] features = new String[] { "feature1" };
    Map<String, Number> m = new HashMap<String, Number>();
    m.put("somemetric", 45);

    cascadingJob = new CascadingJob();
    cascadingJob.setFeatures(features);
    cascadingJob.setConfiguration(properties);
    cascadingJob.setMetrics(m);
  }
  
  @Test
  public void testPigJobRoundTrip() throws IOException {
    doTestRoundTrip(cascadingJob);
  }
  
  private void doTestRoundTrip(CascadingJob expected) throws IOException {
    String asJson = expected.toJson();
    Job asJobAgain = Job.fromJson(asJson);

    // assert that if we get a PigJob without having to ask for it explicitly
    assertTrue(asJobAgain instanceof CascadingJob);
    assertJobEquals(expected, (CascadingJob) asJobAgain);
  }

  public static void assertJobEquals(CascadingJob expected, CascadingJob found) {
    assertEquals(expected.getId(), found.getId());
    assertArrayEquals(expected.getFeatures(), found.getFeatures());
    assertEquals(expected.getMetrics(), found.getMetrics());
    assertEquals(expected.getConfiguration(), found.getConfiguration());
  }
}

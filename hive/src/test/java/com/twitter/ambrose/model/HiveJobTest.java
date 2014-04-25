/*
Copyright 2013, Lorand Bendig

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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Maps;
import com.twitter.ambrose.hive.HiveJob;

/**
 * Unit tests for {@link com.twitter.ambrose.model.HiveJobTest}.
 */
public class HiveJobTest {

  private HiveJob hiveJob;

  @Before
  public void setUp() throws Exception {
    Map<String, Number> metrics = Maps.newHashMapWithExpectedSize(1);
    metrics.put("somemetric", 6);
    Properties properties = new Properties();
    properties.setProperty("someprop", "propvalue");
    String[] aliases = new String[] { "alias1" };
    String[] features = new String[] { "feature1" };
    hiveJob = new HiveJob(aliases, features);
  }

  @Test
  public void testHiveJobRoundTrip() throws IOException {
    doTestRoundTrip(hiveJob);
  }

  private void doTestRoundTrip(HiveJob expected) throws IOException {
    String asJson = expected.toJson();
    Job asJobAgain = Job.fromJson(asJson);

    // assert that if we get a HiveJob without having to ask for it
    // explicitly
    assertTrue(asJobAgain instanceof HiveJob);
    assertJobEquals(expected, (HiveJob) asJobAgain);
  }

  @Test
  public void testDAGNodeHiveJobRoundTrip() throws IOException {
    DAGNode<HiveJob> node = new DAGNode<HiveJob>("dag name", hiveJob);
    doTestRoundTrip(node);
  }

  @Test
  public void testFromJson() throws IOException {
    String json = 
      "{" +
      "  \"type\" : \"JOB_STARTED\"," +
      "  \"payload\" : {" +
      "    \"name\" : \"Stage-1_user_20130723105858_3f0d530c-34a6-4bb9-8964-22c4ea289895\"," +
      "    \"job\" : {" +
      "      \"runtime\" : \"hive\"," +
      "      \"id\" : \"job_201307231015_0004 (Stage-1, query-id: ...22c4ea289895)\"," +
      "      \"aliases\" : [ \"src\" ]," +
      "      \"features\" : [ \"SELECT\", \"FILTER\" ]," +
      "      \"metrics\" : {\n" +
      "        \"somemetric\": 123\n" +
      "      } \n" +
      "    }," +
      "    \"successorNames\" : [ ]" +
      "  }," +
      "  \"id\" : 1," +
      "  \"timestamp\" : 1374569908714" +
      "}, {" +
      "  \"type\" : \"WORKFLOW_PROGRESS\"," +
      "  \"payload\" : {" +
      "    \"workflowProgress\" : \"0\"" +
      "  }," +
      "  \"id\" : 2," +
      "  \"timestamp\" : 1374569908754" +
      "}";

    Event<?> event = Event.fromJson(json);
    @SuppressWarnings("unchecked")
    HiveJob job = ((DAGNode<HiveJob>) event.getPayload()).getJob();
    assertEquals("job_201307231015_0004 (Stage-1, query-id: ...22c4ea289895)", job.getId());
    assertArrayEquals(new String[] { "src" }, job.getAliases());
    assertArrayEquals(new String[] { "SELECT", "FILTER" }, job.getFeatures());
    assertNotNull(job.getMetrics());
    assertEquals(123, job.getMetrics().get("somemetric"));
  }

  private void doTestRoundTrip(DAGNode<HiveJob> expected) throws IOException {
    String asJson = expected.toJson();
    DAGNode<? extends Job> asDAGNodeAgain = DAGNode.fromJson(asJson);
    assertEquals(expected.getName(), asDAGNodeAgain.getName());
    assertNotNull(asDAGNodeAgain.getJob());

    // assert that it's an instance of HiveJob
    assertNotNull(asDAGNodeAgain.getJob() instanceof HiveJob);

    assertJobEquals(expected.getJob(), (HiveJob) asDAGNodeAgain.getJob());
  }

  public static void assertJobEquals(HiveJob expected, HiveJob found) {
    assertEquals(expected.getId(), found.getId());
    assertArrayEquals(expected.getAliases(), found.getAliases());
    assertArrayEquals(expected.getFeatures(), found.getFeatures());
    assertEquals(expected.getMetrics(), found.getMetrics());
    assertEquals(expected.getConfiguration(), found.getConfiguration());
  }
}

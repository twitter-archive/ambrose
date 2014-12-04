package com.twitter.ambrose.model;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.junit.Before;
import org.junit.Test;

import com.twitter.ambrose.pig.PigJob;

/**
 * Unit tests for {@link com.twitter.ambrose.model.PigJobTest}.
 */
public class PigJobTest {
  PigJob pigJob;

  @Before
  public void setUp() throws Exception {
    Properties properties = new Properties();
    properties.setProperty("someprop", "propvalue");
    String[] aliases = new String[] { "alias1" };
    String[] features = new String[] { "feature1" };
    Map<String, Number> m = new HashMap<String, Number>();
    m.put("somemetric", 45);

    pigJob = new PigJob();
    pigJob.setAliases(aliases);
    pigJob.setFeatures(features);
    pigJob.setConfiguration(properties);
    pigJob.setMetrics(m);
  }
  
  @Test
  public void testPigJobRoundTrip() throws IOException {
    doTestRoundTrip(pigJob);
  }

  private void doTestRoundTrip(PigJob expected) throws IOException {
    String asJson = expected.toJson();
    Job asJobAgain = Job.fromJson(asJson);

    // assert that if we get a PigJob without having to ask for it explicitly
    assertTrue(asJobAgain instanceof PigJob);
    assertJobEquals(expected, (PigJob) asJobAgain);
  }

  @Test
  public void testDAGNodePigJobRoundTrip() throws IOException {
    DAGNode<PigJob> node = new DAGNode<PigJob>("dag name", pigJob);
    doTestRoundTrip(node);
  }

  @Test
  public void testFromJson() throws IOException {
    String json =  "{\n" +
                   "  \"type\" : \"JOB_STARTED\",\n" +
                   "  \"payload\" : {\n" +
                   "    \"name\" : \"scope-29\",\n" +
                   "    \"job\" : {\n" +
                   "      \"runtime\" : \"pig\",\n" +
                   "      \"id\" : \"job_local_0001\",\n" +
                   "      \"aliases\" : [ \"A\", \"AA\", \"B\", \"C\" ],\n" +
                   "      \"features\" : [ \"GROUP_BY\", \"COMBINER\", \"MAP_PARTIALAGG\" ],\n" +
                   "      \"metrics\" : {\n" +
                   "        \"somemetric\": 123\n" +
                   "      } \n" +
                   "    },\n" +
                   "    \"successorNames\" : [ ]\n" +
                   "  },\n" +
                   "  \"id\" : 1,\n" +
                   "  \"timestamp\" : 1373560988033\n" +
                   "}";
    Event event = Event.fromJson(json);
    PigJob job = ((DAGNode<PigJob>)event.getPayload()).getJob();
    assertEquals("job_local_0001", job.getId());
    assertArrayEquals(new String[] {"A", "AA", "B", "C"}, job.getAliases());
    assertArrayEquals(new String[] {"GROUP_BY", "COMBINER", "MAP_PARTIALAGG"}, job.getFeatures());
    assertNotNull(job.getMetrics());
    assertEquals(123, job.getMetrics().get("somemetric"));
  }

  private void doTestRoundTrip(DAGNode<PigJob> expected) throws IOException {
    String asJson = expected.toJson();
    DAGNode asDAGNodeAgain = DAGNode.fromJson(asJson);
    assertEquals(expected.getName(), asDAGNodeAgain.getName());
    assertNotNull(asDAGNodeAgain.getJob());

    // assert that it's an instance of PigJob
    assertNotNull(asDAGNodeAgain.getJob() instanceof PigJob);
    assertJobEquals(expected.getJob(), (PigJob)asDAGNodeAgain.getJob());
  }

  public static void assertJobEquals(PigJob expected, PigJob found) {
    assertEquals(expected.getId(), found.getId());
    assertArrayEquals(expected.getAliases(), found.getAliases());
    assertArrayEquals(expected.getFeatures(), found.getFeatures());
    assertEquals(expected.getMetrics(), found.getMetrics());
    assertEquals(expected.getConfiguration(), found.getConfiguration());
  }
}

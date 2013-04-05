package com.twitter.ambrose.model;

import com.twitter.ambrose.pig.PigJob;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link com.twitter.ambrose.model.PigJobTest}.
 */
public class PigJobTest {
  static {
    PigJob.mixinJsonAnnotations();
  }

  PigJob pigJob;

  @Before
  public void setUp() throws Exception {
    Map<String, Number> metrics = new HashMap<String, Number>();
    metrics.put("somemetric", 6);
    Properties properties = new Properties();
    properties.setProperty("someprop", "propvalue");
    String[] aliases = new String[] { "alias1" };
    String[] features = new String[] { "feature1" };
    pigJob = new PigJob(aliases, features);
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

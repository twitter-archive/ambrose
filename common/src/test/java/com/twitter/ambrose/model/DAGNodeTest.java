package com.twitter.ambrose.model;

import java.io.IOException;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Unit tests for {@link DAGNodeTest}.
 */
public class DAGNodeTest {

  private void testRoundTrip(DAGNode expected) throws IOException {
    String asJson = expected.toJson();
    DAGNode asDAGNodeAgain = DAGNode.fromJson(asJson);
    assertEquals(expected.getName(), asDAGNodeAgain.getName());
    assertNotNull(asDAGNodeAgain.getJob());
    ModelTestUtils.assertJobEquals(expected.getJob(), asDAGNodeAgain.getJob());
  }

  @Test
  public void testRoundTrip() throws IOException {
    DAGNode<Job> node = new DAGNode<Job>("dag name", new Job("scope-123", null, null));
    testRoundTrip(node);
  }
}

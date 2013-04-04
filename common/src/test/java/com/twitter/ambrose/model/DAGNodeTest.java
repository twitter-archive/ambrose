package com.twitter.ambrose.model;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.fasterxml.jackson.core.type.TypeReference;

import org.junit.Test;

import com.twitter.ambrose.util.JSONUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Unit tests for {@link DAGNodeTest}.
 */
public class DAGNodeTest {
  private String toJson(DAGNode event) throws IOException {
    return JSONUtil.toJson(event);
  }

  private DAGNode<Job> toDAGNode(String json) throws IOException {
    return JSONUtil.toObject(json, new TypeReference<DAGNode<Job>>() { });
  }

  private void testRoundTrip(DAGNode expected) throws IOException {
    String asJson = toJson(expected);
    System.out.println("DAGNode as json: " + asJson);
    DAGNode asDAGNodeAgain = toDAGNode(asJson);
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

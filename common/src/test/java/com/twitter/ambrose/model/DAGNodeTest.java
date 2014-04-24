package com.twitter.ambrose.model;

import java.io.IOException;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.ImmutableList;
import com.twitter.ambrose.util.JSONUtil;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Unit tests for {@link DAGNodeTest}.
 */
public class DAGNodeTest {
  private void testRoundTrip(DAGNode expected) throws IOException {
    String asJson = expected.toJson();
    System.out.println(asJson);
    DAGNode asDAGNodeAgain = DAGNode.fromJson(asJson);
    assertEquals(expected.getName(), asDAGNodeAgain.getName());
    assertNotNull(asDAGNodeAgain.getJob());
    assertEquals(expected.getJob(), asDAGNodeAgain.getJob());
  }

  @Test
  public void testRoundTrip() throws IOException {
    DAGNode<Job> node = new DAGNode<Job>("scope-1", new Job("job-1", null, null));
    DAGNode<Job> child = new DAGNode<Job>("scope-2", new Job("job-2", null, null));
    node.setSuccessors(ImmutableList.<DAGNode<? extends Job>>of(child));
    testRoundTrip(node);
  }
}
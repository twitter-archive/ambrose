package com.twitter.ambrose.model;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import org.junit.Test;

import com.google.common.collect.Maps;

/**
 * Unit tests for {@link EventTest}.
 */
public class EventTest {
  private void testRoundTrip(Event expected) throws IOException {
    String asJson = expected.toJson();
    System.out.println(asJson);
    Event asEventAgain = Event.fromJson(asJson);
    assertEquals(expected.getId(), asEventAgain.getId());
    assertEquals(expected.getType(), asEventAgain.getType());
    assertEquals(expected.getTimestamp(), asEventAgain.getTimestamp());
    assertTrue(asEventAgain.getPayload() instanceof DAGNode);
    assertEquals(expected.getPayload(), asEventAgain.getPayload());
  }

  @Test
  public void testRoundTrip() throws IOException {
    Properties properties = new Properties();
    properties.setProperty("someprop", "propvalue");
    Map<String, Number> metrics = Maps.newHashMap();
    metrics.put("somemetric", 6);
    Job job = new Job("scope-123", properties, metrics);
    DAGNode<Job> node = new DAGNode<Job>("dag name", job);
    testRoundTrip(new Event.JobStartedEvent(node));
  }

  @Test
  public void testFromJson() throws IOException {
    String json =  "{\n" +
        "  \"type\" : \"JOB_STARTED\",\n" +
        "  \"payload\" : {\n" +
        "    \"name\" : \"scope-29\",\n" +
        "    \"job\" : {\n" +
        "      \"@class\" : \"com.twitter.ambrose.model.Job\",\n" +
        "      \"id\" : \"job_local_0001\",\n" +
        "      \"metrics\" : { \n" +
        "            \"somemetrics\" : 111 \n" +
        "        } \n " +
        "    },\n" +
        "    \"successorNames\" : [ ]\n" +
        "  },\n" +
        "  \"id\" : 1,\n" +
        "  \"timestamp\" : 1373560988033\n" +
        "}";
    Event event = Event.fromJson(json);
    Job job = ((DAGNode<Job>)event.getPayload()).getJob();
    assertEquals("job_local_0001", job.getId());
    assertEquals(111, job.getMetrics().get("somemetrics"));
  }
}

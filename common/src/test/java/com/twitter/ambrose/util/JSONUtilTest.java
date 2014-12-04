package com.twitter.ambrose.util;

import java.io.IOException;
import java.io.PrintWriter;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;

import org.apache.hadoop.io.DataOutputBuffer;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Unit tests for JSONUtil.
 */
public class JSONUtilTest {
  public void test(Object obj, String expected) throws IOException {
    DataOutputBuffer out = new DataOutputBuffer();
    PrintWriter writer = new PrintWriter(out);
    JSONUtil.writeJson(writer, obj);
    String actual = new String(out.getData(), 0, out.getLength(), Charsets.UTF_8.name());
    assertEquals(expected, actual);
  }

  @Test
  public void test() throws IOException {
    test("Testing", "\"Testing\"");
    test(ImmutableMap.of("key", "value"), "{\n  \"key\" : \"value\"\n}");
  }
}

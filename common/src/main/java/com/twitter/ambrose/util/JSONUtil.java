/*
Copyright 2012 Twitter, Inc.

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
package com.twitter.ambrose.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.reflections.Reflections;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.reflect.Reflection;
import com.twitter.ambrose.model.Job;

/**
 * Helper method for dealing with JSON in a common way.
 *
 * @author billg
 */
public class JSONUtil {
  
  private JSONUtil() {}
  /**
   * Writes object to the writer as JSON using Jackson and adds a new-line before flushing.
   *
   * @param writer the writer to write the JSON to
   * @param object the object to write as JSON
   * @throws IOException if the object can't be serialized as JSON or written to the writer
   */
  public static void writeJson(Writer writer, Object object) throws IOException {
    mapper.writeValue(writer, object);
  }

  public static void writeJson(String fileName, Object object) throws IOException {
    Writer writer = new PrintWriter(fileName);
    try {
      JSONUtil.writeJson(writer, object);
    } finally {
      writer.close();
    }
  }

  /**
   * Serializes object to JSON string.
   *
   * @param object object to serialize.
   * @return json string.
   * @throws IOException
   */
  public static String toJson(Object object) throws IOException {
    StringWriter writer = new StringWriter();
    writeJson(writer, object);
    return writer.toString();
  }

  /**
   * Parse JSON string to object.
   *
   * @param json string containing JSON.
   * @param type type reference describing type of object to parse from json.
   * @param <T> type of object to parse from json.
   * @return object parsed from json.
   * @throws IOException
   */
  public static <T> T toObject(String json, TypeReference<T> type) throws IOException {
      try {
        return mapper.readValue(json, type);
      } catch (JsonParseException e) {
        throw new IOException(String.format("Failed to parse json '%s'", json), e);
      }
  }

  public static <T> T toObject(String json, JavaType type) throws IOException {
    return mapper.readValue(json, type);
  }

  public static String readFile(String path) throws IOException {
    FileInputStream stream = new FileInputStream(new File(path));
    try {
      FileChannel fc = stream.getChannel();
      MappedByteBuffer bb = fc.map(FileChannel.MapMode.READ_ONLY, 0, fc.size());
      return Charset.defaultCharset().decode(bb).toString();
    } finally {
      stream.close();
    }
  }

  private static final ObjectMapper mapper = newMapper();

  private static ObjectMapper newMapper() {
    ObjectMapper mapper = new ObjectMapper();
    mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    mapper.enable(SerializationFeature.INDENT_OUTPUT);
    mapper.configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);
    mapper.configure(JsonGenerator.Feature.AUTO_CLOSE_JSON_CONTENT, false);
    mapper.disable(SerializationFeature.FLUSH_AFTER_WRITE_VALUE);
    mapper.disable(SerializationFeature.CLOSE_CLOSEABLE);
    mapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
    mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    Reflections reflections = new Reflections("com.twitter.ambrose");
    Set<Class<? extends Job>> jobSubTypes = reflections.getSubTypesOf(Job.class);
    mapper.registerSubtypes(jobSubTypes.toArray(new Class<?>[jobSubTypes.size()]));
    return mapper;
  }
}

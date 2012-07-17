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

import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.codehaus.jackson.type.TypeReference;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;

/**
 * Helper method for dealing with JSON in a common way.
 *
 * @author billg
 */
public class JSONUtil {

  /**
   * Writes object to the writer as JSON using Jackson and adds a new-line before flushing.
   * @param writer the writer to write the JSON to
   * @param object the object to write as JSON
   * @throws IOException if the object can't be serialized as JSON or written to the writer
   */
  public static void writeJson(Writer writer, Object object) throws IOException {
    ObjectMapper om = new ObjectMapper();
    om.configure(SerializationConfig.Feature.INDENT_OUTPUT, true);
    om.configure(SerializationConfig.Feature.FAIL_ON_EMPTY_BEANS, false);

    writer.write(om.writeValueAsString(object));
    writer.write("\n");
    writer.flush();
  }

  public static void writeJson(String fileName, Object object) throws IOException {
    JSONUtil.writeJson(new PrintWriter(fileName), object);
  }

  public static Object readJson(String json, TypeReference type) throws IOException {
    ObjectMapper om = new ObjectMapper();
    om.getDeserializationConfig().set(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    // not currently setting successors, only successorNames
    return om.readValue(json, type);
  }

  public static String readFile(String path) throws IOException {
    FileInputStream stream = new FileInputStream(new File(path));
    try {
      FileChannel fc = stream.getChannel();
      MappedByteBuffer bb = fc.map(FileChannel.MapMode.READ_ONLY, 0, fc.size());
      return Charset.defaultCharset().decode(bb).toString();
    }
    finally {
      stream.close();
    }
  }


}

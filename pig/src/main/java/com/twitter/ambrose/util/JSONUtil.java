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

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;

import java.io.IOException;
import java.io.Writer;

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
}

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.http.util;

import java.io.FileInputStream;
import java.io.IOException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;

public class JsonConverter {
  public static JsonNode parse(String content, String key) {
    String []path = key.split("/");
    try {
      JsonNode node = from(content);
      for (String p : path) {
        if (node == null) {
          return null;
        }
        node = node.get(p);
      }
      return node;
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }

  public static JsonNode parse(String content) {
    try {
      JsonNode root = from(content);
      if (root.isArray()) {
        return root;
      }
      return null;
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }

  @SuppressWarnings("resource")
  public static String stringFromFile(String file) {
    try {
      FileInputStream stream = new FileInputStream(file);
      int size = stream.available();
      byte[] bytes = new byte[size];
      stream.read(bytes);
      return new String(bytes, Charsets.UTF_8);
    } catch (IOException e) {
    }
    return "";
  }

 private static JsonNode from(String content) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    JsonNode root = mapper.readTree(content);
    return root;
  }
}

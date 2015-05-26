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
package org.apache.drill.exec.store.http;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;

public class HttpScanSpec {
  private String uri;
  private Map<String, Object> args = new HashMap<String, Object>();

  @JsonCreator
  public HttpScanSpec(@JsonProperty("uri") String uri) {
    this.uri = uri;
  }

  public HttpScanSpec(String uri, String key, Object val) {
    this.uri = uri;
    this.args.put(key, val);
  }

  public String getURI() {
    return uri;
  }

  public String getURL() {
    if (args.size() == 0) {
      return uri;
    }
    Joiner j = Joiner.on('&');
    return uri + '?' + j.withKeyValueSeparator("=").join(args);
  }

  public void merge(HttpScanSpec that) {
    for (Map.Entry<String, Object> entry : that.args.entrySet()) {
      this.args.put(entry.getKey(), entry.getValue());
    }
  }

  @Override
  public String toString() {
    return "HttpScanSpec [uri='" + uri + "', args=" + args + "]";
  }
}

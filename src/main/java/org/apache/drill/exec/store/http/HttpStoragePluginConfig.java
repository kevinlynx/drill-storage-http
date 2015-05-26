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

import org.apache.drill.common.logical.StoragePluginConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

/* created from drill web storage configuration */
@JsonTypeName(HttpStoragePluginConfig.NAME)
public class HttpStoragePluginConfig extends StoragePluginConfig {
  static final Logger logger = LoggerFactory.getLogger(HttpStoragePluginConfig.class);

  public static final String NAME = "http";
  private String connection;

  @JsonCreator
  public HttpStoragePluginConfig(@JsonProperty("connection") String connection) {
    logger.info("initialize HttpStoragePluginConfig {}", connection);
    this.connection = connection;
  }

  @Override
  public boolean equals(Object that) {
    if (this == that) {
      return true;
    } else if (that == null || getClass() != that.getClass()) {
      return false;
    }
    logger.debug("HttpStoragePluginConfig equals {}", connection);
    HttpStoragePluginConfig t = (HttpStoragePluginConfig) that;
    return this.connection.equals(t.connection);
  }

  @Override
  public int hashCode() {
    return this.connection != null ? this.connection.hashCode() : 0;
  }

  @Override
  public String toString() {
    return "HttpStoragePluginConfig[connection=" + connection + "]";
  }

  public String getConnection() {
    return connection;
  }
}

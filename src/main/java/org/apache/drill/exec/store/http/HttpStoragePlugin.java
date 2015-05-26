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

import java.io.IOException;
import java.util.Set;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.StoragePluginOptimizerRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class HttpStoragePlugin extends AbstractStoragePlugin {
  static final Logger logger = LoggerFactory.getLogger(HttpStoragePlugin.class);

  private DrillbitContext context;
  private HttpStoragePluginConfig httpConfig;
  private HttpSchemaFactory schemaFactory;

  public HttpStoragePlugin(HttpStoragePluginConfig httpConfig,
      DrillbitContext context, String name) throws IOException,
      ExecutionSetupException {
    logger.debug("initialize HttpStoragePlugin {} {}", name, httpConfig);
    this.context = context;
    this.httpConfig = httpConfig;
    this.schemaFactory = new HttpSchemaFactory(this, name);
  }

  public DrillbitContext getContext() {
    return this.context;
  }

  @Override
  public HttpStoragePluginConfig getConfig() {
    return httpConfig;
  }

  @Override
  public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws IOException {
    schemaFactory.registerSchemas(schemaConfig, parent);
  }

  @Override
  public boolean supportsRead() {
    return true;
  }

  @Override
  public AbstractGroupScan getPhysicalScan(String userName, JSONOptions selection) throws IOException {
    /* selection only represent database and collection name */
    HttpScanSpec spec = selection.getListWith(new ObjectMapper(), new TypeReference<HttpScanSpec>() {});
    logger.debug("getPhysicalScan {} {} {} {}", userName, selection, selection.getRoot(), spec);
    return new HttpGroupScan(userName, httpConfig, spec);
  }

  public Set<StoragePluginOptimizerRule> getOptimizerRules() {
    return ImmutableSet.of(HttpPushDownFilterForScan.INSTANCE);
  }

}

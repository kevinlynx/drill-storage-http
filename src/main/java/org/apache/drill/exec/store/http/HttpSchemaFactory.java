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
import org.apache.calcite.schema.Table;
import org.apache.drill.exec.planner.logical.DynamicDrillTable;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.SchemaFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;


public class HttpSchemaFactory implements SchemaFactory{
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HttpSchemaFactory.class);

  private final HttpStoragePlugin plugin;
  private final String schemaName;

  public HttpSchemaFactory(HttpStoragePlugin plugin, String schemaName) {
    super();
    this.plugin = plugin;
    this.schemaName = schemaName;
  }

  @Override
  public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws IOException {
    logger.debug("registerSchema {}", schemaName);
    HttpSchema schema = new HttpSchema(schemaName);
    parent.add(schema.getName(), schema);
  }

  public class HttpSchema extends AbstractSchema {
    private Set<String> tableNames = Sets.newHashSet();

    public HttpSchema(String name) {
      super(ImmutableList.<String> of(), name);
      tableNames.add("static"); // TODO: not necessary
    }

    @Override
    public String getTypeName() {
      return HttpStoragePluginConfig.NAME;
    }

    @Override
    public Set<String> getTableNames() {
      return tableNames;
    }

    @Override
    public Table getTable(String tableName) { // table name can be any of string
      logger.debug("HttpSchema.getTable {}", tableName);
      HttpScanSpec spec = new HttpScanSpec(tableName); // will be pass to getPhysicalScan
      return new DynamicDrillTable(plugin, schemaName, null, spec);
    }
  }
}

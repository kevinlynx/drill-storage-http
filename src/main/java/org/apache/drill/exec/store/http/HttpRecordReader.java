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
import java.util.Iterator;
import java.util.Set;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.http.util.JsonConverter;
import org.apache.drill.exec.store.http.util.SimpleHttp;
import org.apache.drill.exec.vector.BaseValueVector;
import org.apache.drill.exec.vector.complex.fn.JsonReader;
import org.apache.drill.exec.vector.complex.impl.VectorContainerWriter;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class HttpRecordReader extends AbstractRecordReader {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HttpRecordReader.class);

  private VectorContainerWriter writer;
  private JsonReader jsonReader;
  private FragmentContext fragmentContext;
  private HttpSubScan subScan;
  private Iterator<JsonNode> jsonIt;

  public HttpRecordReader(FragmentContext context, HttpSubScan subScan) {
    this.subScan = subScan;
    fragmentContext = context;
    Set<SchemaPath> transformed = Sets.newLinkedHashSet();
    transformed.add(STAR_COLUMN);
    setColumns(transformed);
  }

  @Override
  public void setup(OperatorContext context, OutputMutator output)
      throws ExecutionSetupException {
    logger.debug("HttpRecordReader setup, query {}", subScan.getFullURL());
    this.writer = new VectorContainerWriter(output);
    this.jsonReader = new JsonReader(fragmentContext.getManagedBuffer(),
      Lists.newArrayList(getColumns()), true, false, true);
    String q = subScan.getURL();
    if (q.startsWith("file://")) {
      loadFile();
    } else {
      loadHttp();
    }
  }

  private void loadHttp() {
    String url = subScan.getFullURL();
    SimpleHttp http = new SimpleHttp();
    String content = http.get(url);
    logger.debug("http '{}' response {} bytes", url, content.length());
    parseResult(content);
  }

  private void loadFile() {
    logger.debug("load local file {}", subScan.getScanSpec().getURI());
    String file = subScan.getScanSpec().getURI().substring("file://".length() - 1);
    String content = JsonConverter.stringFromFile(file);
    parseResult(content);
  }

  private void parseResult(String content) {
    String key = subScan.getStorageConfig().getResultKey();
    JsonNode root = key.length() == 0 ? JsonConverter.parse(content) :
      JsonConverter.parse(content, key);
    if (root != null) {
      logger.debug("response object count {}", root.size());
      jsonIt = root.elements();
    }
  }

  @Override
  public int next() {
    logger.debug("HttpRecordReader next");
    if (jsonIt == null || !jsonIt.hasNext()) {
      return 0;
    }
    writer.allocate();
    writer.reset();
    int docCount = 0;
    try {
      while (docCount < BaseValueVector.INITIAL_VALUE_ALLOCATION && jsonIt.hasNext()) {
        JsonNode node = jsonIt.next();
        jsonReader.setSource(node.toString().getBytes(Charsets.UTF_8));
        writer.setPosition(docCount);
        jsonReader.write(writer);
        docCount ++;
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    writer.setValueCount(docCount);
    return docCount;
  }

  @Override
  public void cleanup() {
    logger.debug("HttpRecordReader cleanup");
  }

}

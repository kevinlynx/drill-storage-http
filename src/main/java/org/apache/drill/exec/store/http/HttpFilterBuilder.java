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

import java.util.List;

import org.apache.drill.common.expression.BooleanOperator;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.visitors.AbstractExprVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpFilterBuilder extends
    AbstractExprVisitor<HttpScanSpec, Void, RuntimeException> {
  static final Logger logger = LoggerFactory.getLogger(HttpFilterBuilder.class);
  private final HttpGroupScan groupScan;
  private final LogicalExpression le;
  private boolean allExpressionsConverted = true;

  public boolean isAllExpressionsConverted() {
    return allExpressionsConverted;
  }

  public HttpFilterBuilder(HttpGroupScan groupScan, LogicalExpression conditionExp) {
    this.groupScan = groupScan;
    this.le = conditionExp;
    logger.debug("HttpFilterBuilder created");
  }

  public HttpScanSpec parseTree() {
    HttpScanSpec parsedSpec = le.accept(this, null);
    if (parsedSpec != null) {
      parsedSpec = mergeScanSpecs(this.groupScan.getScanSpec(), parsedSpec);
    }
    return parsedSpec;
  }

  private HttpScanSpec mergeScanSpecs(HttpScanSpec leftScanSpec, HttpScanSpec rightScanSpec) {
    leftScanSpec.merge(rightScanSpec);
    return leftScanSpec;
  }

  @Override
  public HttpScanSpec visitUnknown(LogicalExpression e, Void value)
      throws RuntimeException {
    allExpressionsConverted = false;
    return null;
  }

  // only process `boolean and` expression
  // `a and b and c` will call this, and argument size = 3
  @Override
  public HttpScanSpec visitBooleanOperator(BooleanOperator op, Void value) {
    List<LogicalExpression> args = op.args;
    HttpScanSpec nodeScanSpec = null;
    String functionName = op.getName();
    if (!functionName.equals("booleanAnd")) {
      allExpressionsConverted = false;
      return nodeScanSpec;
    }
    logger.debug("boolean 'and' operator {}", args.size());
    for (int i = 0; i < args.size(); ++i) {
      if (nodeScanSpec == null) {
        nodeScanSpec = args.get(i).accept(this, null);
      } else {
        HttpScanSpec scanSpec = args.get(i).accept(this, null);
        if (scanSpec != null) {
          nodeScanSpec = mergeScanSpecs(nodeScanSpec, scanSpec);
        } else {
          allExpressionsConverted = false;
        }
      }
    }
    return nodeScanSpec;
  }

  // only process expression like `$key=value`
  @Override
  public HttpScanSpec visitFunctionCall(FunctionCall call, Void value) throws RuntimeException {
    HttpScanSpec nodeScanSpec = null;
    String functionName = call.getName();
    logger.debug("visit function call {}", functionName);
    if (!HttpEqualFunctionProcessor.match(functionName)) {
      allExpressionsConverted = false;
      return nodeScanSpec;
    }
    HttpEqualFunctionProcessor processor = HttpEqualFunctionProcessor.process(call);
    if (!processor.isSuccess()) {
      allExpressionsConverted = false;
      return nodeScanSpec;
    }
    logger.debug("visit equal function {}={}", processor.getPath().getAsUnescapedPath(), processor.getValue());
    nodeScanSpec = createHttpScanSpec(processor.getPath(), processor.getValue());
    return nodeScanSpec;
  }

  private HttpScanSpec createHttpScanSpec(SchemaPath field, Object fieldValue) {
    String fieldName = field.getAsUnescapedPath();
    if (fieldName.charAt(0) != '$') { // HTTP query string starts with $
      return null;
    }
    String queryKey = fieldName.substring(1);
    return new HttpScanSpec(groupScan.getScanSpec().getURI(), queryKey, fieldValue);
  }
}

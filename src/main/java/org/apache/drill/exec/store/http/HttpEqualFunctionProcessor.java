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

import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.ValueExpressions.BooleanExpression;
import org.apache.drill.common.expression.ValueExpressions.DoubleExpression;
import org.apache.drill.common.expression.ValueExpressions.FloatExpression;
import org.apache.drill.common.expression.ValueExpressions.IntExpression;
import org.apache.drill.common.expression.ValueExpressions.LongExpression;
import org.apache.drill.common.expression.ValueExpressions.QuotedString;
import org.apache.drill.common.expression.visitors.AbstractExprVisitor;

public class HttpEqualFunctionProcessor extends
  AbstractExprVisitor<Boolean, LogicalExpression, RuntimeException> {
  private Object value;
  private SchemaPath path;
  private boolean success;

  static boolean match(String functionName) {
    return functionName.equals("equal");
  }

  public static HttpEqualFunctionProcessor process(FunctionCall call) {
    LogicalExpression nameArg = call.args.get(0);
    LogicalExpression valueArg = call.args.get(1);
    HttpEqualFunctionProcessor evaluator = new HttpEqualFunctionProcessor();

    evaluator.success = nameArg.accept(evaluator, valueArg);

    return evaluator;
  }

  public HttpEqualFunctionProcessor() {
  }

  public Object getValue() {
    return value;
  }

  public boolean isSuccess() {
    return success;
  }

  public SchemaPath getPath() {
    return path;
  }

  @Override
  public Boolean visitUnknown(LogicalExpression e, LogicalExpression valueArg)
      throws RuntimeException {
    return false;
  }

  @Override
  public Boolean visitSchemaPath(SchemaPath path, LogicalExpression valueArg)
      throws RuntimeException {
    if (valueArg instanceof QuotedString) {
      this.value = ((QuotedString) valueArg).value;
      this.path = path;
      return true;
    }

    if (valueArg instanceof IntExpression) {
      this.value = ((IntExpression) valueArg).getInt();
      this.path = path;
      return true;
    }

    if (valueArg instanceof LongExpression) {
      this.value = ((LongExpression) valueArg).getLong();
      this.path = path;
      return true;
    }

    if (valueArg instanceof FloatExpression) {
      this.value = ((FloatExpression) valueArg).getFloat();
      this.path = path;
      return true;
    }

    if (valueArg instanceof DoubleExpression) {
      this.value = ((DoubleExpression) valueArg).getDouble();
      this.path = path;
      return true;
    }

    if (valueArg instanceof BooleanExpression) {
      this.value = ((BooleanExpression) valueArg).getBoolean();
      this.path = path;
      return true;
    }

    return false;
  }

}

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

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rex.RexNode;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.exec.planner.logical.DrillOptiq;
import org.apache.drill.exec.planner.logical.DrillParseContext;
import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.apache.drill.exec.planner.physical.FilterPrel;
import org.apache.drill.exec.planner.physical.PrelUtil;
import org.apache.drill.exec.planner.physical.ScanPrel;
import org.apache.drill.exec.store.StoragePluginOptimizerRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpPushDownFilterForScan extends StoragePluginOptimizerRule {
  private static final Logger logger = LoggerFactory .getLogger(HttpPushDownFilterForScan.class);
  public static final StoragePluginOptimizerRule INSTANCE = new HttpPushDownFilterForScan();

  private HttpPushDownFilterForScan() {
    super(
        RelOptHelper.some(FilterPrel.class, RelOptHelper.any(ScanPrel.class)),
        "HttpPushDownFilterForScan");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final ScanPrel scan = (ScanPrel) call.rel(1);
    final FilterPrel filter = (FilterPrel) call.rel(0);
    final RexNode condition = filter.getCondition();
    logger.debug("Http onMatch");

    HttpGroupScan groupScan = (HttpGroupScan) scan.getGroupScan();
    if (groupScan.isFilterPushedDown()) {
      return;
    }

    LogicalExpression conditionExp = DrillOptiq.toDrill(
        new DrillParseContext(PrelUtil.getPlannerSettings(call.getPlanner())), scan, condition);
    HttpFilterBuilder httpFilterBuilder = new HttpFilterBuilder(groupScan, conditionExp);
    HttpScanSpec newScanSpec = httpFilterBuilder.parseTree();
    if (newScanSpec == null) {
      return; // no filter pushdown so nothing to apply.
    }

    HttpGroupScan newGroupsScan = null;
    newGroupsScan = new HttpGroupScan(groupScan.getUserName(), groupScan.getStorageConfig(), newScanSpec);
    newGroupsScan.setFilterPushedDown(true);
    logger.debug("assign new group scan with spec {}", newScanSpec);
    final ScanPrel newScanPrel = ScanPrel.create(scan, filter.getTraitSet(),
        newGroupsScan, scan.getRowType());
    call.transformTo(newScanPrel);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    final ScanPrel scan = (ScanPrel) call.rel(1);
    if (scan.getGroupScan() instanceof HttpGroupScan) {
      return super.matches(call);
    }
    return false;
  }
}

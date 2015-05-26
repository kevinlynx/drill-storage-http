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

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.PhysicalOperatorSetupException;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.physical.base.ScanStats.GroupScanProperty;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;

public class HttpGroupScan extends AbstractGroupScan {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HttpGroupScan.class);

  private HttpScanSpec scanSpec;
  private HttpStoragePluginConfig config;
  private boolean filterPushedDown = false;

  public HttpGroupScan(String userName, HttpStoragePluginConfig config, HttpScanSpec spec) {
    super(userName);
    scanSpec = spec;
    this.config = config;
  }

  public HttpGroupScan(HttpGroupScan that) {
    super(that);
    scanSpec = that.scanSpec;
    config = that.config;
  }

  public HttpScanSpec getScanSpec() {
    return scanSpec;
  }

  @Override
  public boolean canPushdownProjects(List<SchemaPath> columns) {
    return true;
  }

  @Override
  public SubScan getSpecificScan(int minorFragmentId) { // pass to HttpScanBatchCreator
    logger.debug("HttpGroupScan getSpecificScan");
    return new HttpSubScan(config, scanSpec);
  }

  @Override
  public int getMaxParallelizationWidth() {
    return 0;
  }

  @Override
  public GroupScan clone(List<SchemaPath> columns) {
    // selection columns from here
    logger.debug("HttpGroupScan clone {}", columns);
    return this;
  }

  @Override
  public String getDigest() {
    return toString();
  }

  @Override
  public void applyAssignments(List<DrillbitEndpoint> endpoints)
      throws PhysicalOperatorSetupException {
    logger.debug("HttpGroupScan applyAssignments");
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children)
      throws ExecutionSetupException {
    logger.debug("HttpGroupScan getNewWithChildren");
    return new HttpGroupScan(this);
  }

  @Override
  public ScanStats getScanStats() {
    return new ScanStats(GroupScanProperty.EXACT_ROW_COUNT, 1, 1, (float) 10);
  }

  public boolean isFilterPushedDown() {
    return filterPushedDown;
  }

  public void setFilterPushedDown(boolean filterPushedDown) {
    this.filterPushedDown = filterPushedDown;
  }

  public HttpStoragePluginConfig getStorageConfig() {
    return config;
  }

}

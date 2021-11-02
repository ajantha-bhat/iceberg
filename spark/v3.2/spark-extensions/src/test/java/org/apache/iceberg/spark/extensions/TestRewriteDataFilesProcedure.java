/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.spark.extensions;

import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.catalyst.analysis.NoSuchProcedureException;
import org.junit.After;
import org.junit.Test;


public class TestRewriteDataFilesProcedure extends SparkExtensionsTestBase {

  public TestRewriteDataFilesProcedure(String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @After
  public void removeTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @Test
  public void testRewriteDataFilesInEmptyTable() {
    createTable();
    List<Object[]> output = sql(
        "CALL %s.system.rewrite_data_files('%s')", catalogName, tableIdent);
    assertEquals("Procedure output must match",
        ImmutableList.of(row(0, 0)),
        output);
  }

  @Test
  public void testRewriteDataFilesOnPartitionTable() {
    createPartitionTable();
    // create 5 files for each partition (data = 'foo' and data = 'bar')
    insertData(5);
    List<Object[]> expectedRecords = currentData();

    List<Object[]> output = sql(
            "CALL %s.system.rewrite_data_files(table => '%s')", catalogName, tableIdent);

    assertEquals("Action should rewrite 10 data files and add 2 data files (one per partition) ",
            ImmutableList.of(row(10, 2)),
            output);

    List<Object[]> actualRecords = currentData();
    assertEquals("data should match", expectedRecords, actualRecords);
  }

  @Test
  public void testRewriteDataFilesOnNonPartitionTable() {
    createTable();
    // create 10 files under non-partitioned table
    insertData(5);
    List<Object[]> expectedRecords = currentData();

    List<Object[]> output = sql(
            "CALL %s.system.rewrite_data_files(table => '%s')", catalogName, tableIdent);

    assertEquals("Action should rewrite 10 data files and add 1 data files",
            ImmutableList.of(row(10, 1)),
            output);

    List<Object[]> actualRecords = currentData();
    assertEquals("data should match", expectedRecords, actualRecords);
  }

  @Test
  public void testRewriteDataFilesWithOptions() {
    createTable();
    // create 2 files under non-partitioned table
    insertData(1);
    List<Object[]> expectedRecords = currentData();

    // set the min-input-files = 2, instead of default 5 to allow compaction
    List<Object[]> output = sql(
            "CALL %s.system.rewrite_data_files(table => '%s', options => map('min-input-files','2'))",
            catalogName, tableIdent);

    assertEquals("Action should rewrite 2 data files and add 1 data files",
            ImmutableList.of(row(2, 1)),
            output);

    List<Object[]> actualRecords = currentData();
    assertEquals("data should match", expectedRecords, actualRecords);
  }

  @Test
  public void testRewriteDataFilesWithSortStrategy() {
    createTable();
    // create 2 files under non-partitioned table
    insertData(1);
    List<Object[]> expectedRecords = currentData();

    // set the min-input-files = 2, instead of default 5 to allow compaction
    // set sort_order = id DESC LAST
    List<Object[]> output = sql(
            "CALL %s.system.rewrite_data_files(table => '%s', options => map('min-input-files','2'), " +
                    "strategy => 'sort', sort_order => 'id DESC NULLS_LAST')",
            catalogName, tableIdent);

    assertEquals("Action should rewrite 2 data files and add 1 data files",
            ImmutableList.of(row(2, 1)),
            output);

    List<Object[]> actualRecords = currentData();
    assertEquals("data should match", expectedRecords, actualRecords);
  }

  @Test
  public void testRewriteDataFilesWithFilter() {
    createTable();
    // create 10 files under non-partitioned table
    insertData(5);
    List<Object[]> expectedRecords = currentData();

    // select only 5 files for compaction (files that may have id = 1)
    List<Object[]> output = sql(
            "CALL %s.system.rewrite_data_files(table => '%s'," +
                    " where => 'id = 1 and data is not null')", catalogName, tableIdent);

    assertEquals("Action should rewrite 5 data files (containing id = 1) and add 1 data files",
            ImmutableList.of(row(5, 1)),
            output);

    List<Object[]> actualRecords = currentData();
    assertEquals("data should match", expectedRecords, actualRecords);
  }

  @Test
  public void testRewriteDataFilesWithFilterOnPartitionTable() {
    createPartitionTable();
    // create 5 files for each partition (data = 'foo' and data = 'bar')
    insertData(5);
    List<Object[]> expectedRecords = currentData();

    // select only 5 files for compaction (files in the partition data = 'bar')
    List<Object[]> output = sql(
        "CALL %s.system.rewrite_data_files(table => '%s'," +
            " where => 'data = \"bar\"')", catalogName, tableIdent);

    assertEquals("Action should rewrite 5 data files from single matching partition" +
            "(containing data = bar) and add 1 data files",
        ImmutableList.of(row(5, 1)),
        output);

    List<Object[]> actualRecords = currentData();
    assertEquals("data should match", expectedRecords, actualRecords);
  }

  @Test
  public void testRewriteDataFilesWithInvalidInputs() {
    createTable();
    // create 2 files under non-partitioned table
    insertData(1);
    List<Object[]> expectedRecords = currentData();

    // Test for invalid strategy
    AssertHelpers.assertThrows("Should reject calls with unsupported strategy error message",
            IllegalArgumentException.class, "unsupported strategy: temp. Only binpack,sort is supported",
            () -> sql("CALL %s.system.rewrite_data_files(table => '%s', options => map('min-input-files','2'), " +
                            "strategy => 'temp')", catalogName, tableIdent));

    // Test for sort_order with binpack strategy
    AssertHelpers.assertThrows("Should reject calls with error message",
            IllegalArgumentException.class, "Cannot set strategy to sort, it has already been set",
            () -> sql("CALL %s.system.rewrite_data_files(table => '%s', strategy => 'binpack', " +
                    "sort_order => 'id ASC NULLS_FIRST')", catalogName, tableIdent));

    // Test for sort_order with invalid null order
    AssertHelpers.assertThrows("Should reject calls with error message",
            IllegalArgumentException.class, "sort_order_column is having invalid null order: none",
            () -> sql("CALL %s.system.rewrite_data_files(table => '%s', strategy => 'sort', " +
                    "sort_order => 'id ASC none')", catalogName, tableIdent));

    // Test for sort_order with invalid sort direction
    AssertHelpers.assertThrows("Should reject calls with error message",
            IllegalArgumentException.class, "sort_order_column is having invalid direction: none",
            () -> sql("CALL %s.system.rewrite_data_files(table => '%s', strategy => 'sort', " +
                    "sort_order => 'id none NULLS_FIRST')", catalogName, tableIdent));

    // Test for sort_order with invalid sort_order_column
    AssertHelpers.assertThrows("Should reject calls with error message",
            IllegalArgumentException.class, "sort_order_column: id should have 3 members " +
                    "separated by space",
            () -> sql("CALL %s.system.rewrite_data_files(table => '%s', strategy => 'sort', " +
                    "sort_order => 'id')", catalogName, tableIdent));

    // Test for sort_order with invalid column name
    AssertHelpers.assertThrows("Should reject calls with error message",
            ValidationException.class, "Cannot find field 'col1' in struct:" +
                    " struct<1: id: required long, 2: data: optional string>",
            () -> sql("CALL %s.system.rewrite_data_files(table => '%s', strategy => 'sort', " +
                    "sort_order => 'col1 DESC NULLS_FIRST')", catalogName, tableIdent));

    // Test for sort_order with invalid filter column col1
    AssertHelpers.assertThrows("Should reject calls with error message",
            ValidationException.class, "Cannot find field 'col1' in struct:" +
                    " struct<1: id: required long, 2: data: optional string>",
            () -> sql("CALL %s.system.rewrite_data_files(table => '%s', " +
                    "where => 'col1 = 3')", catalogName, tableIdent));
  }

  @Test
  public void testInvalidCasesForRewriteDataFiles() {
    AssertHelpers.assertThrows("Should not allow mixed args",
        AnalysisException.class, "Named and positional arguments cannot be mixed",
        () -> sql("CALL %s.system.rewrite_data_files('n', table => 't')", catalogName));

    AssertHelpers.assertThrows("Should not resolve procedures in arbitrary namespaces",
        NoSuchProcedureException.class, "not found",
        () -> sql("CALL %s.custom.rewrite_data_files('n', 't')", catalogName));

    AssertHelpers.assertThrows("Should reject calls without all required args",
        AnalysisException.class, "Missing required parameters",
        () -> sql("CALL %s.system.rewrite_data_files()", catalogName));

    AssertHelpers.assertThrows("Should reject duplicate arg names name",
        AnalysisException.class, "Duplicate procedure argument: table",
        () -> sql("CALL %s.system.rewrite_data_files(table => 't', table => 't')", catalogName));

    AssertHelpers.assertThrows("Should reject calls with empty table identifier",
        IllegalArgumentException.class, "Cannot handle an empty identifier",
        () -> sql("CALL %s.system.rewrite_data_files('')", catalogName));
  }

  private void createTable() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
  }

  private void createPartitionTable() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg PARTITIONED BY (data)", tableName);
  }

  private void insertData(int repeatCounter) {
    IntStream.range(0, repeatCounter).forEach(i -> {
      sql("INSERT INTO TABLE %s VALUES (1, 'foo')", tableName);
      sql("INSERT INTO TABLE %s VALUES (2, 'bar')", tableName);
    });
  }

  private List<Object[]> currentData() {
    return rowsToJava(spark.sql("SELECT * FROM " + tableName + " order by id, data").collectAsList());
  }

}

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
package org.apache.iceberg.flink.sink.shuffle;

import org.apache.flink.table.data.GenericRowData;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortDirection;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.flink.DataGenerator;
import org.apache.iceberg.flink.DataGenerators;

public class TestSortKeySerializerPrimitives extends TestSortKeySerializerBase {
  private final DataGenerator generator = new DataGenerators.Primitives();

  @Override
  protected Schema schema() {
    return generator.icebergSchema();
  }

  @Override
  protected SortOrder sortOrder() {
    return SortOrder.builderFor(schema())
        .asc("boolean_field")
        .sortBy(Expressions.bucket("int_field", 4), SortDirection.DESC, NullOrder.NULLS_LAST)
        .sortBy(Expressions.truncate("string_field", 2), SortDirection.ASC, NullOrder.NULLS_FIRST)
        .sortBy(Expressions.bucket("uuid_field", 16), SortDirection.ASC, NullOrder.NULLS_FIRST)
        .sortBy(Expressions.hour("ts_with_zone_field"), SortDirection.ASC, NullOrder.NULLS_FIRST)
        .sortBy(Expressions.day("ts_without_zone_field"), SortDirection.ASC, NullOrder.NULLS_FIRST)
        // can not test HeapByteBuffer due to equality test inside SerializerTestBase
        // .sortBy(Expressions.truncate("binary_field", 2), SortDirection.ASC,
        // NullOrder.NULLS_FIRST)
        .build();
  }

  @Override
  protected GenericRowData rowData() {
    return generator.generateFlinkRowData();
  }
}

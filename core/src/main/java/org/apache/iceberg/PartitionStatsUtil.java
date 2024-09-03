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
package org.apache.iceberg;

import java.util.Map;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.PartitionUtil;

public class PartitionStatsUtil {

  private PartitionStatsUtil() {}

  public enum Column {
    PARTITION,
    SPEC_ID,
    DATA_RECORD_COUNT,
    DATA_FILE_COUNT,
    TOTAL_DATA_FILE_SIZE_IN_BYTES,
    POSITION_DELETE_RECORD_COUNT,
    POSITION_DELETE_FILE_COUNT,
    EQUALITY_DELETE_RECORD_COUNT,
    EQUALITY_DELETE_FILE_COUNT,
    TOTAL_RECORD_COUNT,
    LAST_UPDATED_AT,
    LAST_UPDATED_SNAPSHOT_ID
  }

  /**
   * Generates a Schema object as per partition statistics spec based on the given partition type.
   *
   * @param partitionType the struct type that defines the structure of the partition.
   * @return a Schema object that corresponds to the provided partition type.
   */
  public static Schema schema(Types.StructType partitionType) {
    if (partitionType.fields().isEmpty()) {
      throw new IllegalArgumentException("getting schema for an unpartitioned table");
    }

    return new Schema(
        Types.NestedField.required(1, Column.PARTITION.name(), partitionType),
        Types.NestedField.required(2, Column.SPEC_ID.name(), Types.IntegerType.get()),
        Types.NestedField.required(3, Column.DATA_RECORD_COUNT.name(), Types.LongType.get()),
        Types.NestedField.required(4, Column.DATA_FILE_COUNT.name(), Types.IntegerType.get()),
        Types.NestedField.required(
            5, Column.TOTAL_DATA_FILE_SIZE_IN_BYTES.name(), Types.LongType.get()),
        Types.NestedField.optional(
            6, Column.POSITION_DELETE_RECORD_COUNT.name(), Types.LongType.get()),
        Types.NestedField.optional(
            7, Column.POSITION_DELETE_FILE_COUNT.name(), Types.IntegerType.get()),
        Types.NestedField.optional(
            8, Column.EQUALITY_DELETE_RECORD_COUNT.name(), Types.LongType.get()),
        Types.NestedField.optional(
            9, Column.EQUALITY_DELETE_FILE_COUNT.name(), Types.IntegerType.get()),
        Types.NestedField.optional(10, Column.TOTAL_RECORD_COUNT.name(), Types.LongType.get()),
        Types.NestedField.optional(11, Column.LAST_UPDATED_AT.name(), Types.LongType.get()),
        Types.NestedField.optional(
            12, Column.LAST_UPDATED_SNAPSHOT_ID.name(), Types.LongType.get()));
  }

  /**
   * Creates an iterable of partition stats records from a given manifest file, using the specified
   * table and record schema.
   *
   * @param table the table from which the manifest file is derived.
   * @param manifest the manifest file containing metadata about the records.
   * @param recordSchema the schema defining the structure of the records.
   * @return a CloseableIterable of partition stats records as defined by the manifest file and
   *     record schema.
   */
  public static CloseableIterable<Record> fromManifest(
      Table table, ManifestFile manifest, Schema recordSchema) {
    return CloseableIterable.transform(
        ManifestFiles.open(manifest, table.io(), table.specs())
            .select(BaseScan.scanColumns(manifest.content()))
            .liveEntries(),
        entry -> fromManifestEntry(entry, table, recordSchema));
  }

  /**
   * Appends statistics from one Record to another.
   *
   * @param toRecord the Record to which statistics will be appended.
   * @param fromRecord the Record from which statistics will be sourced.
   */
  public static void appendStatsFromRecord(Record toRecord, Record fromRecord) {
    Preconditions.checkState(toRecord != null, "Record to update cannot be null");
    Preconditions.checkState(fromRecord != null, "Record to update from cannot be null");

    toRecord.set(
        Column.SPEC_ID.ordinal(),
        Math.max(
            (int) toRecord.get(Column.SPEC_ID.ordinal()),
            (int) fromRecord.get(Column.SPEC_ID.ordinal())));
    checkAndIncrementLong(toRecord, fromRecord, Column.DATA_RECORD_COUNT);
    checkAndIncrementInt(toRecord, fromRecord, Column.DATA_FILE_COUNT);
    checkAndIncrementLong(toRecord, fromRecord, Column.TOTAL_DATA_FILE_SIZE_IN_BYTES);
    checkAndIncrementLong(toRecord, fromRecord, Column.POSITION_DELETE_RECORD_COUNT);
    checkAndIncrementInt(toRecord, fromRecord, Column.POSITION_DELETE_FILE_COUNT);
    checkAndIncrementLong(toRecord, fromRecord, Column.EQUALITY_DELETE_RECORD_COUNT);
    checkAndIncrementInt(toRecord, fromRecord, Column.EQUALITY_DELETE_FILE_COUNT);
    checkAndIncrementLong(toRecord, fromRecord, Column.TOTAL_RECORD_COUNT);
    if (fromRecord.get(Column.LAST_UPDATED_AT.ordinal()) != null) {
      if (toRecord.get(Column.LAST_UPDATED_AT.ordinal()) == null
          || ((long) toRecord.get(Column.LAST_UPDATED_AT.ordinal())
              < (long) fromRecord.get(Column.LAST_UPDATED_AT.ordinal()))) {
        toRecord.set(
            Column.LAST_UPDATED_AT.ordinal(), fromRecord.get(Column.LAST_UPDATED_AT.ordinal()));
        toRecord.set(
            Column.LAST_UPDATED_SNAPSHOT_ID.ordinal(),
            fromRecord.get(Column.LAST_UPDATED_SNAPSHOT_ID.ordinal()));
      }
    }
  }

  /**
   * Converts the given {@link PartitionData} into a {@link Record} based on the specified partition
   * schema.
   *
   * @param partitionSchema the schema defining the structure of the partition data.
   * @param partitionData the data to be converted into a Record.
   * @return a Record that represents the partition data as per the given schema.
   */
  public static Record partitionDataToRecord(
      Types.StructType partitionSchema, PartitionData partitionData) {
    GenericRecord genericRecord = GenericRecord.create(partitionSchema);
    for (int index = 0; index < partitionData.size(); index++) {
      genericRecord.set(index, partitionData.get(index));
    }

    return genericRecord;
  }

  private static Record fromManifestEntry(
      ManifestEntry<?> entry, Table table, Schema recordSchema) {
    GenericRecord record = GenericRecord.create(recordSchema);
    Types.StructType partitionType =
        recordSchema.findField(Column.PARTITION.name()).type().asStructType();
    PartitionData partitionData = coercedPartitionData(entry.file(), table.specs(), partitionType);
    record.set(Column.PARTITION.ordinal(), partitionDataToRecord(partitionType, partitionData));
    record.set(Column.SPEC_ID.ordinal(), entry.file().specId());

    Snapshot snapshot = table.snapshot(entry.snapshotId());
    if (snapshot != null) {
      record.set(Column.LAST_UPDATED_SNAPSHOT_ID.ordinal(), snapshot.snapshotId());
      record.set(Column.LAST_UPDATED_AT.ordinal(), snapshot.timestampMillis());
    }

    switch (entry.file().content()) {
      case DATA:
        record.set(Column.DATA_FILE_COUNT.ordinal(), 1);
        record.set(Column.DATA_RECORD_COUNT.ordinal(), entry.file().recordCount());
        record.set(Column.TOTAL_DATA_FILE_SIZE_IN_BYTES.ordinal(), entry.file().fileSizeInBytes());
        break;
      case POSITION_DELETES:
        record.set(Column.POSITION_DELETE_FILE_COUNT.ordinal(), 1);
        record.set(Column.POSITION_DELETE_RECORD_COUNT.ordinal(), entry.file().recordCount());
        break;
      case EQUALITY_DELETES:
        record.set(Column.EQUALITY_DELETE_FILE_COUNT.ordinal(), 1);
        record.set(Column.EQUALITY_DELETE_RECORD_COUNT.ordinal(), entry.file().recordCount());
        break;
      default:
        throw new UnsupportedOperationException(
            "Unsupported file content type: " + entry.file().content());
    }

    // TODO: optionally compute TOTAL_RECORD_COUNT based on the flag
    return record;
  }

  private static PartitionData coercedPartitionData(
      ContentFile<?> file, Map<Integer, PartitionSpec> specs, Types.StructType partitionType) {
    // keep the partition data as per the unified spec by coercing
    StructLike partition =
        PartitionUtil.coercePartition(partitionType, specs.get(file.specId()), file.partition());
    PartitionData data = new PartitionData(partitionType);
    for (int i = 0; i < partitionType.fields().size(); i++) {
      Object val = partition.get(i, partitionType.fields().get(i).type().typeId().javaClass());
      if (val != null) {
        data.set(i, val);
      }
    }
    return data;
  }

  private static void checkAndIncrementLong(Record toUpdate, Record fromRecord, Column column) {
    if (fromRecord.get(column.ordinal()) != null) {
      if (toUpdate.get(column.ordinal()) != null) {
        toUpdate.set(
            column.ordinal(),
            toUpdate.get(column.ordinal(), Long.class)
                + fromRecord.get(column.ordinal(), Long.class));
      } else {
        toUpdate.set(column.ordinal(), fromRecord.get(column.ordinal(), Long.class));
      }
    }
  }

  private static void checkAndIncrementInt(Record toUpdate, Record fromRecord, Column column) {
    if (toUpdate.get(column.ordinal()) != null && fromRecord.get(column.ordinal()) != null) {
      toUpdate.set(
          column.ordinal(),
          toUpdate.get(column.ordinal(), Integer.class)
              + fromRecord.get(column.ordinal(), Integer.class));
    }
  }
}

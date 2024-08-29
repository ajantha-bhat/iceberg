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
package org.apache.iceberg.data;

import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiFunction;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ImmutableGenericPartitionStatisticsFile;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionStatisticsFile;
import org.apache.iceberg.PartitionStats;
import org.apache.iceberg.PartitionStatsUtil;
import org.apache.iceberg.Partitioning;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.avro.DataReader;
import org.apache.iceberg.data.orc.GenericOrcReader;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.encryption.EncryptionKeyMetadata;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.FileWriterFactory;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SnapshotUtil;

/**
 * Util to write and read the {@link PartitionStatisticsFile}. Uses generic readers and writes to
 * support writing and reading of the stats in table default format.
 */
public final class PartitionStatsHandler {

  private PartitionStatsHandler() {}

  public enum Column {
    PARTITION(0),
    SPEC_ID(1),
    DATA_RECORD_COUNT(2),
    DATA_FILE_COUNT(3),
    TOTAL_DATA_FILE_SIZE_IN_BYTES(4),
    POSITION_DELETE_RECORD_COUNT(5),
    POSITION_DELETE_FILE_COUNT(6),
    EQUALITY_DELETE_RECORD_COUNT(7),
    EQUALITY_DELETE_FILE_COUNT(8),
    TOTAL_RECORD_COUNT(9),
    LAST_UPDATED_AT(10),
    LAST_UPDATED_SNAPSHOT_ID(11);

    private final int id;

    Column(int id) {
      this.id = id;
    }

    public int id() {
      return id;
    }
  }

  /**
   * Generates the Partition Stats Files Schema based on a given partition type.
   *
   * <p>Note: Provide the unified partition tuple as mentioned in the spec.
   *
   * @param partitionType the struct type that defines the structure of the partition.
   * @return a schema that corresponds to the provided unified partition type.
   */
  public static Schema schema(Types.StructType partitionType) {
    Preconditions.checkState(
        !partitionType.fields().isEmpty(), "getting schema for an unpartitioned table");

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

  public static PartitionStatisticsFile writePartitionStatsFile(Table table) {
    return writePartitionStatsFile(table, null);
  }

  public static PartitionStatisticsFile writePartitionStatsFile(Table table, String branch) {
    Snapshot currentSnapshot = SnapshotUtil.latestSnapshot(table, branch);
    if (currentSnapshot == null) {
      Preconditions.checkArgument(
          branch == null, "Couldn't find the snapshot for the branch %s", branch);
      return null;
    }

    Types.StructType partitionType = Partitioning.partitionType(table);
    Schema dataSchema = schema(partitionType);

    Iterable<PartitionStats> partitionStats =
        PartitionStatsUtil.computeStats(table, currentSnapshot);
    List<PartitionStats> sortedStats = PartitionStatsUtil.sortStats(partitionStats, partitionType);
    Iterator<Record> convertedRecords = partitionStatsToRecords(sortedStats.iterator(), dataSchema);

    OutputFile outputFile = newPartitionStatsFile(table, currentSnapshot.snapshotId());
    FileFormat fileFormat =
        FileFormat.fromString(
            outputFile.location().substring(outputFile.location().lastIndexOf(".") + 1));
    FileWriterFactory<Record> factory =
        GenericFileWriterFactory.builderFor(table)
            .dataSchema(dataSchema)
            .dataFileFormat(fileFormat)
            .build();
    DataWriter<Record> writer =
        factory.newDataWriter(
            EncryptedFiles.encryptedOutput(outputFile, EncryptionKeyMetadata.EMPTY),
            PartitionSpec.unpartitioned(),
            null);
    try (Closeable toClose = writer) {
      convertedRecords.forEachRemaining(writer::write);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    return ImmutableGenericPartitionStatisticsFile.builder()
        .snapshotId(currentSnapshot.snapshotId())
        .path(outputFile.location())
        .fileSizeInBytes(outputFile.toInputFile().getLength())
        .build();
  }

  /**
   * Reads partition statistics from the specified {@link InputFile} using given schema.
   *
   * @param schema The {@link Schema} of the partition statistics file.
   * @param inputFile An {@link InputFile} pointing to the partition stats file.
   */
  public static CloseableIterable<PartitionStats> readPartitionStatsFile(
      Schema schema, InputFile inputFile) {
    FileFormat fileFormat =
        FileFormat.fromString(
            inputFile.location().substring(inputFile.location().lastIndexOf(".") + 1));

    CloseableIterable<Record> records;
    switch (fileFormat) {
      case PARQUET:
        records =
            Parquet.read(inputFile)
                .project(schema)
                .createReaderFunc(
                    fileSchema -> GenericParquetReaders.buildReader(schema, fileSchema))
                .build();
        break;
      case ORC:
        records =
            ORC.read(inputFile)
                .project(schema)
                .createReaderFunc(fileSchema -> GenericOrcReader.buildReader(schema, fileSchema))
                .build();
        break;
      case AVRO:
        records = Avro.read(inputFile).project(schema).createReaderFunc(DataReader::create).build();
        break;
      default:
        throw new UnsupportedOperationException("Unsupported file format:" + fileFormat.name());
    }

    return CloseableIterable.transform(records, PartitionStatsHandler::recordToPartitionStats);
  }

  private static OutputFile newPartitionStatsFile(Table table, long snapshotId) {
    FileFormat fileFormat =
        FileFormat.fromString(
            table.properties().getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT));
    return table
        .io()
        .newOutputFile(
            ((HasTableOperations) table)
                .operations()
                .metadataFileLocation(
                    fileFormat.addExtension(String.format("partition-stats-%d", snapshotId))));
  }

  private static PartitionStats recordToPartitionStats(Record record) {
    PartitionStats partitionStats = new PartitionStats( PartitionStatsRecord.recordToPartitionData(record));
    partitionStats.setSpecId(record.get(Column.SPEC_ID.id(), Integer.class));
    partitionStats.setDataRecordCount(record.get(Column.DATA_RECORD_COUNT.id(), Long.class));
    partitionStats.setDataFileCount(record.get(Column.DATA_FILE_COUNT.id(), Integer.class));
    partitionStats.setTotalDataFileSizeInBytes(
            record.get(Column.TOTAL_DATA_FILE_SIZE_IN_BYTES.id(), Long.class));
    partitionStats.setPositionDeleteRecordCount(
            record.get(Column.POSITION_DELETE_RECORD_COUNT.id(), Long.class));
    partitionStats.setPositionDeleteFileCount(
            record.get(Column.POSITION_DELETE_FILE_COUNT.id(), Integer.class));
    partitionStats.setEqualityDeleteRecordCount(
            record.get(Column.EQUALITY_DELETE_RECORD_COUNT.id(), Long.class));
    partitionStats.setEqualityDeleteFileCount(
            record.get(Column.EQUALITY_DELETE_FILE_COUNT.id(), Integer.class));
    partitionStats.setTotalRecordCount(record.get(Column.TOTAL_RECORD_COUNT.id(), Long.class));
    partitionStats.setLastUpdatedAt(record.get(Column.LAST_UPDATED_AT.id(), Long.class));
    partitionStats.setLastUpdatedSnapshotId(
            record.get(Column.LAST_UPDATED_SNAPSHOT_ID.id(), Long.class));

    return partitionStats;
  }

  private static Iterator<Record> partitionStatsToRecords(
      Iterator<PartitionStats> partitionStatsIterator, Schema recordSchema) {
    return new TransformIteratorWithBiFunction<>(
        partitionStatsIterator,
        (partitionStats, schema) -> {
          PartitionStatsRecord record = PartitionStatsRecord.create(schema, partitionStats);
          PartitionData partitionData = record.get(Column.PARTITION.id(), PartitionData.class);
          Record partitionRecord =
                  PartitionStatsRecord.partitionDataToRecord(
                  partitionData, (Types.StructType) schema.findType(Column.PARTITION.name()));
          record.set(Column.PARTITION.id(), partitionRecord);

          return record;
        },
        recordSchema);
  }

  private static class TransformIteratorWithBiFunction<T, U, R> implements Iterator<R> {
    private final Iterator<T> iterator;
    private final BiFunction<T, U, R> transformer;
    private final U additionalInput;

    TransformIteratorWithBiFunction(
        Iterator<T> iterator, BiFunction<T, U, R> transformer, U additionalInput) {
      this.iterator = iterator;
      this.transformer = transformer;
      this.additionalInput = additionalInput;
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public R next() {
      T nextElement = iterator.next();
      return transformer.apply(nextElement, additionalInput);
    }
  }
}

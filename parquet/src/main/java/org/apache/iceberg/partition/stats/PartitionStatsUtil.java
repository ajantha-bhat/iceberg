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
package org.apache.iceberg.partition.stats;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.UUID;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Partitioning;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types;

public class PartitionStatsUtil {

  private PartitionStatsUtil() {}

  public static String writePartitionStatsFile(Table table) {

    Types.StructType partitionType = Partitioning.partitionType(table);
    if (partitionType.fields().isEmpty()) {
      throw new UnsupportedOperationException(
          "table " + table.name() + " is an unpartitioned table");
    }

    Schema schema =
        new Schema(
            Types.NestedField.required(1, "partition", partitionType),
            Types.NestedField.required(2, "spec_id", Types.IntegerType.get()),
            Types.NestedField.required(3, "data_record_count", Types.LongType.get()),
            Types.NestedField.required(4, "data_file_count", Types.IntegerType.get()),
            Types.NestedField.required(5, "position_delete_record_count", Types.LongType.get()),
            Types.NestedField.required(6, "position_delete_file_count", Types.IntegerType.get()),
            Types.NestedField.required(7, "equality_delete_record_count", Types.LongType.get()),
            Types.NestedField.required(8, "equality_delete_file_count", Types.IntegerType.get()));

    String dataFileFormatName =
        table
            .properties()
            .getOrDefault(
                TableProperties.DEFAULT_FILE_FORMAT, TableProperties.DEFAULT_FILE_FORMAT_DEFAULT);
    FileFormat format = FileFormat.fromString(dataFileFormatName);

    Path metadataPath =
        Paths.get(table.location() + "/metadata/" + UUID.randomUUID() + "." + format.name());

    ImmutableList.Builder<Record> builder = ImmutableList.builder();

    Record nestedStruct = GenericRecord.create(partitionType);
    nestedStruct.setField("id", 1L);

    Record record = GenericRecord.create(schema);
    record.setField("partition", nestedStruct);
    record.setField("spec_id", 1);
    record.setField("data_record_count", 2L);
    record.setField("data_file_count", 3);
    record.setField("position_delete_record_count", 0L);
    record.setField("position_delete_file_count", 0);
    record.setField("equality_delete_record_count", 0L);
    record.setField("equality_delete_file_count", 0);

    for (int i = 0; i < 500; i++) {
      builder.add(record);
    }

    List<Record> records = builder.build();

    ((BaseTable) table).operations().current().metadataFileLocation();

    switch (format) {
      case PARQUET:
        writeAsParquetFile(schema, records, Files.localOutput(metadataPath.toFile()));
        break;
      default:
        throw new UnsupportedOperationException(
            "table " + table.name() + " is an unpartitioned table");
    }

    // TODO: follow the output path style
    return metadataPath.toString();
  }

  // define a schema
  // collect the rows and write into a single partition stats file in a table default format

  private static void writeAsParquetFile(
      Schema schema, List<Record> records, OutputFile outputFile) {

    Types.NestedField firstPartition =
        ((Types.StructType) schema.asStruct().fields().get(0).type()).fields().get(0);
    SortOrder sortOrder =
        SortOrder.builderFor(schema).asc("partition." + firstPartition.name()).build();

    try (DataWriter<Record> dataWriter =
        Parquet.writeData(outputFile)
            .schema(schema)
            .createWriterFunc(GenericParquetWriter::buildWriter)
            .overwrite()
            .withSpec(PartitionSpec.unpartitioned())
            .withSortOrder(sortOrder)
            .build()) {
      records.forEach(dataWriter::write);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}

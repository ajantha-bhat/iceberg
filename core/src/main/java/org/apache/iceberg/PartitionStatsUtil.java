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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.PartitionUtil;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PartitionStatsUtil {

  private PartitionStatsUtil() {}

  private static final Logger LOG = LoggerFactory.getLogger(PartitionStatsUtil.class);

  public static Iterable<PartitionStats> computeStats(Table table, Snapshot snapshot) {
    Types.StructType partitionType = Partitioning.partitionType(table);
    Map<PartitionData, PartitionStats> partitionEntryMap = Maps.newConcurrentMap();

    List<ManifestFile> manifestFiles = snapshot.allManifests(table.io());
    Tasks.foreach(manifestFiles)
        .stopOnFailure()
        .executeWith(ThreadPools.getWorkerPool())
        .onFailure(
            (file, thrown) ->
                LOG.warn(
                    "Failed to compute the partition stats for the manifest file: {}",
                    file.path(),
                    thrown))
        .run(
            manifest -> {
              try (CloseableIterable<PartitionStats> entries =
                  PartitionStatsUtil.fromManifest(table, manifest, partitionType)) {
                entries.forEach(
                    entry -> {
                      PartitionData partitionKey = entry.partition();
                      partitionEntryMap.merge(
                          partitionKey,
                          entry,
                          (existingEntry, newEntry) -> {
                            existingEntry.appendStats(newEntry);
                            return existingEntry;
                          });
                    });
              } catch (IOException e) {
                throw new UncheckedIOException(e);
              }
            });

    return partitionEntryMap.values();
  }

  public static List<PartitionStats> sortStats(
      Iterable<PartitionStats> stats, Types.StructType partitionType) {
    List<PartitionStats> entries = Lists.newArrayList(stats.iterator());
    entries.sort(
        Comparator.comparing(PartitionStats::partition, Comparators.forType(partitionType)));
    return entries;
  }

  private static CloseableIterable<PartitionStats> fromManifest(
      Table table, ManifestFile manifest, Types.StructType partitionType) {
    return CloseableIterable.transform(
        ManifestFiles.open(manifest, table.io(), table.specs())
            .select(BaseScan.scanColumns(manifest.content()))
            .entries(),
        entry -> {
          // partition data as per unified partition spec
          PartitionData partitionData =
              coercedPartitionData(entry.file(), table.specs(), partitionType);
          PartitionStats partitionStats = new PartitionStats(partitionData);
          if (entry.isLive()) {
            partitionStats.liveEntry(entry.file(), table.snapshot(entry.snapshotId()));
          } else {
            partitionStats.deletedEntry(table.snapshot(entry.snapshotId()));
          }

          return partitionStats;
        });
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
}

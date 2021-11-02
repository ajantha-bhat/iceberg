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

package org.apache.iceberg.spark.procedures;

import java.util.Map;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.RewriteDataFiles;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.SparkExpressions;
import org.apache.iceberg.spark.procedures.SparkProcedures.ProcedureBuilder;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.catalyst.parser.ParserInterface;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.iceberg.catalog.ProcedureParameter;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.runtime.BoxedUnit;

/**
 * A procedure that rewrites datafiles in a table.
 *
 * @see org.apache.iceberg.spark.actions.SparkActions#rewriteDataFiles(Table)
 */
class RewriteDataFilesProcedure extends BaseProcedure {

  private static final ProcedureParameter[] PARAMETERS = new ProcedureParameter[]{
          ProcedureParameter.required("table", DataTypes.StringType),
          ProcedureParameter.optional("strategy", DataTypes.StringType),
          ProcedureParameter.optional("sort_order", DataTypes.StringType),
          ProcedureParameter.optional("options", STRING_MAP),
          ProcedureParameter.optional("where", DataTypes.StringType)
  };

  // counts are not nullable since the action result is never null
  private static final StructType OUTPUT_TYPE = new StructType(new StructField[]{
      new StructField("rewritten_data_files_count", DataTypes.IntegerType, false, Metadata.empty()),
      new StructField("added_data_files_count", DataTypes.IntegerType, false, Metadata.empty())
  });

  public static ProcedureBuilder builder() {
    return new Builder<RewriteDataFilesProcedure>() {
      @Override
      protected RewriteDataFilesProcedure doBuild() {
        return new RewriteDataFilesProcedure(tableCatalog());
      }
    };
  }

  private RewriteDataFilesProcedure(TableCatalog tableCatalog) {
    super(tableCatalog);
  }

  @Override
  public ProcedureParameter[] parameters() {
    return PARAMETERS;
  }

  @Override
  public StructType outputType() {
    return OUTPUT_TYPE;
  }

  @Override
  public InternalRow[] call(InternalRow args) {
    Identifier tableIdent = toIdentifier(args.getString(0), PARAMETERS[0].name());

    return modifyIcebergTable(tableIdent, table -> {
      RewriteDataFiles action = actions().rewriteDataFiles(table);

      String strategy = args.isNullAt(1) ? null : args.getString(1);
      SortOrder sortOrder = collectSortOrders(table, args.isNullAt(2) ? null : args.getString(2));
      action = checkAndApplyStrategy(action, strategy, sortOrder);

      if (!args.isNullAt(3)) {
        action = checkAndApplyOptions(args, action);
      }

      String where = args.isNullAt(4) ? null : args.getString(4);
      action = checkAndApplyFilter(action, where);

      RewriteDataFiles.Result result = action.execute();

      return toOutputRows(result);
    });
  }

  private RewriteDataFiles checkAndApplyFilter(RewriteDataFiles action, String where) {
    if (where != null) {
      ParserInterface sqlParser = spark().sessionState().sqlParser();
      try {
        Expression expression = sqlParser.parseExpression(where);
        return action.filter(SparkExpressions.convert(expression));
      } catch (ParseException e) {
        throw new IllegalArgumentException("Cannot parse predicates in where option: " + where);
      }
    }
    return action;
  }

  private RewriteDataFiles checkAndApplyOptions(InternalRow args, RewriteDataFiles action) {
    Map<String, String> options = Maps.newHashMap();
    args.getMap(3).foreach(DataTypes.StringType, DataTypes.StringType,
            (k, v) -> {
              options.put(k.toString(), v.toString());
              return BoxedUnit.UNIT;
            });
    return action.options(options);
  }

  private RewriteDataFiles checkAndApplyStrategy(RewriteDataFiles action, String strategy, SortOrder sortOrder) {
    if (strategy != null) {
      if (strategy.equalsIgnoreCase("binpack")) {
        RewriteDataFiles rewriteDataFiles = action.binPack();
        if (sortOrder != null) {
          // calling below method to throw the error as user has set both binpack strategy and sortorder
          return rewriteDataFiles.sort(sortOrder);
        }
        return rewriteDataFiles;
      } else if (strategy.equalsIgnoreCase("sort")) {
        if (sortOrder == null) {
          return action.sort();
        }
        return action.sort(sortOrder);
      } else {
        throw new IllegalArgumentException("unsupported strategy: " + strategy + ". Only binpack,sort is supported");
      }
    } else {
      // overerride default binpack to sort, as startgey is not set but sort order is set.
      if (sortOrder != null) {
        return action.sort(sortOrder);
      }
    }
    return action;
  }

  private SortOrder collectSortOrders(Table table, String sortOrderStr) {
    if (sortOrderStr != null) {
      SortOrder.Builder builder = SortOrder.builderFor(table.schema());
      String[] sortOrders = sortOrderStr.split(",");
      for (String sortOrderColumnStr : sortOrders) {
        String[] sortOrderColumn = sortOrderColumnStr.split(" ");
        if (sortOrderColumn.length != 3) {
          throw new IllegalArgumentException(
                  "sort_order_column: " + sortOrderColumnStr + " should have 3 members separated by space");
        }
        if (sortOrderColumn[2].equalsIgnoreCase("nulls_first")) {
          if (sortOrderColumn[1].equalsIgnoreCase("asc")) {
            builder.asc(sortOrderColumn[0], NullOrder.NULLS_FIRST);
          } else if (sortOrderColumn[1].equalsIgnoreCase("desc")) {
            builder.desc(sortOrderColumn[0], NullOrder.NULLS_FIRST);
          } else {
            throw new IllegalArgumentException(
                "sort_order_column is having invalid direction: " + sortOrderColumn[1] + "only ASC,DESC is " +
                    "supported");
          }
        } else if (sortOrderColumn[2].equalsIgnoreCase("nulls_last")) {
          if (sortOrderColumn[1].equalsIgnoreCase("asc")) {
            builder.asc(sortOrderColumn[0], NullOrder.NULLS_LAST);
          } else if (sortOrderColumn[1].equalsIgnoreCase("desc")) {
            builder.desc(sortOrderColumn[0], NullOrder.NULLS_LAST);
          } else {
            throw new IllegalArgumentException(
                "sort_order_column is having invalid direction: " + sortOrderColumn[1] + "only ASC,DESC is " +
                    "supported");
          }
        } else {
          throw new IllegalArgumentException("sort_order_column is having invalid null order: " + sortOrderColumn[2] +
              "only NULLS_FIRST,NULLS_LAST is supported");
        }
      }
      return builder.build();
    }
    return null;
  }

  private InternalRow[] toOutputRows(RewriteDataFiles.Result result) {
    int rewrittenDataFilesCount = result.rewrittenDataFilesCount();
    int addedDataFilesCount = result.addedDataFilesCount();
    InternalRow row = newInternalRow(rewrittenDataFilesCount, addedDataFilesCount);
    return new InternalRow[]{row};
  }

  @Override
  public String description() {
    return "RewriteDataFilesProcedure";
  }
}

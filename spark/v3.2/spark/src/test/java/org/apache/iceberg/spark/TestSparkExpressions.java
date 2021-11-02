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

package org.apache.iceberg.spark;

import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.expressions.Expressions;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.catalyst.parser.ParserInterface;
import org.junit.Assert;
import org.junit.Test;

public class TestSparkExpressions extends SparkTestBase {

  @Test
  public void testBasicExpressions() {
    org.apache.iceberg.expressions.Expression expected = Expressions.equal("x", 5);
    org.apache.iceberg.expressions.Expression actual = SparkExpressions.convert(getSparkExpression("x = 5"));
    validateResult(expected, actual);

    expected = Expressions.not(Expressions.equal("x", "abc"));
    actual = SparkExpressions.convert(getSparkExpression("x != 'abc'"));
    validateResult(expected, actual);

    expected = Expressions.lessThan("x", 5);
    actual = SparkExpressions.convert(getSparkExpression("x < 5"));
    validateResult(expected, actual);

    expected = Expressions.lessThanOrEqual("x", -3);
    actual = SparkExpressions.convert(getSparkExpression("x <= -3"));
    validateResult(expected, actual);

    expected = Expressions.greaterThan("x", 0);
    actual = SparkExpressions.convert(getSparkExpression("x > 0"));
    validateResult(expected, actual);

    expected = Expressions.greaterThanOrEqual("x", 129);
    actual = SparkExpressions.convert(getSparkExpression("x >= 129"));
    validateResult(expected, actual);

    expected = Expressions.greaterThanOrEqual("x", 129);
    actual = SparkExpressions.convert(getSparkExpression("x >= 129"));
    validateResult(expected, actual);

    expected = Expressions.and(Expressions.greaterThanOrEqual("x", 3), Expressions.lessThan("y", 5));
    actual = SparkExpressions.convert(getSparkExpression("x >= 3 AND y < 5"));
    validateResult(expected, actual);

    expected = Expressions.or(Expressions.greaterThanOrEqual("x", 3), Expressions.lessThan("y", 5));
    actual = SparkExpressions.convert(getSparkExpression("x >= 3 OR y < 5"));
    validateResult(expected, actual);

    expected = Expressions.isNull("x");
    actual = SparkExpressions.convert(getSparkExpression("x is null"));
    validateResult(expected, actual);

    expected = Expressions.or(Expressions.equal("x", 1), Expressions.equal("x", 2));
    actual = SparkExpressions.convert(getSparkExpression("x in (1,2)"));
    validateResult(expected, actual);

    expected =
        Expressions.not(Expressions.or(Expressions.equal("x", 1), Expressions.equal("x", 2)));
    actual = SparkExpressions.convert(getSparkExpression("x not in (1,2)"));
    validateResult(expected, actual);

    expected =
        Expressions.and(Expressions.greaterThanOrEqual("x", 1), Expressions.lessThanOrEqual("x", 3));
    actual = SparkExpressions.convert(getSparkExpression("x between 1 and 3"));
    validateResult(expected, actual);
  }

  @Test
  public void testUnsupportedOperations() {
    // like filter
    AssertHelpers.assertThrows("Like expression is not supported",
        IllegalArgumentException.class, "Unsupported expression: class org.apache.spark.sql.catalyst.expressions.Like",
        () -> SparkExpressions.convert(getSparkExpression("x like '%abc'")));
  }

  // TODO: Add testcases for date, timestamp and all supported transforms (code doesn't handle truncate, bucket, hour)

  private void validateResult(
      org.apache.iceberg.expressions.Expression expected,
      org.apache.iceberg.expressions.Expression actual) {
    Assert.assertEquals("expressions should match", expected.toString(), actual.toString());
  }

  private Expression getSparkExpression(String expressionString) {
    ParserInterface sqlParser = spark.sessionState().sqlParser();
    try {
      return sqlParser.parseExpression(expressionString);
    } catch (ParseException e) {
      throw new IllegalArgumentException("Cannot parse expression string: " + expressionString);
    }
  }
}

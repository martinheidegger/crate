/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.operation.projectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import io.crate.analyze.symbol.Literal;
import io.crate.core.collections.Bucket;
import io.crate.core.collections.Row;
import io.crate.operation.Input;
import io.crate.operation.collect.CollectExpression;
import io.crate.operation.collect.InputCollectExpression;
import io.crate.operation.projectors.sorting.OrderingByPosition;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.CollectingRowReceiver;
import io.crate.testing.RowSender;
import org.junit.Test;

import java.util.List;

import static io.crate.testing.TestingHelpers.isRow;
import static org.hamcrest.core.Is.is;

public class SortingTopNProjectorTest extends CrateUnitTest {

    private static final InputCollectExpression INPUT = new InputCollectExpression(0);
    private static final Literal<Boolean> TRUE_LITERAL = Literal.of(true);
    private static final List<Input<?>> INPUT_LITERAL_LIST = ImmutableList.of(INPUT, TRUE_LITERAL);
    private static final List<CollectExpression<Row, ?>> COLLECT_EXPRESSIONS = ImmutableList.<CollectExpression<Row, ?>>of(INPUT);
    private static final Ordering<Object[]> FIRST_CELL_ORDERING = OrderingByPosition.arrayOrdering(0, false, null);

    private Projector getProjector(int numOutputs, int limit, int offset, RowReceiver rowReceiver, Ordering<Object[]> ordering) {
        Projector pipe = new SortingTopNProjector(
            INPUT_LITERAL_LIST,
            COLLECT_EXPRESSIONS,
            numOutputs,
            ordering,
            limit,
            offset
        );
        pipe.downstream(rowReceiver);
        return pipe;
    }

    private Projector getProjector(int numOutputs, int limit, int offset, RowReceiver rowReceiver) {
        return getProjector(numOutputs, limit, offset, rowReceiver, FIRST_CELL_ORDERING);
    }

    @Test
    public void testOrderBy() throws Exception {
        CollectingRowReceiver rowReceiver = new CollectingRowReceiver();
        Projector pipe = getProjector(1, 3, 5, rowReceiver);
        RowSender.generateRowsInRangeAndEmit(1, 11, pipe);

        Bucket rows = rowReceiver.result();
        assertThat(rows.size(), is(3));

        long iterateLength = 0;
        for (Row row : rows) {
            assertThat(row, isRow(iterateLength + 6));
            iterateLength++;
        }
        assertThat(iterateLength, is(3L));
    }

    @Test
    public void testOrderByWithoutOffset() throws Exception {
        CollectingRowReceiver rowReceiver = new CollectingRowReceiver();
        Projector pipe = getProjector(2, 10, TopN.NO_OFFSET, rowReceiver);
        RowSender.generateRowsInRangeAndEmit(1, 11, pipe);

        Bucket rows = rowReceiver.result();
        assertThat(rows.size(), is(10));
        long iterateLength = 0;
        for (Row row : rowReceiver.result()) {
            assertThat(row, isRow(iterateLength + 1, true));
            iterateLength++;
        }
        assertThat(iterateLength, is(10L));
    }

    @Test
    public void testWithHighOffset() throws Exception {
        CollectingRowReceiver rowReceiver = new CollectingRowReceiver();
        Projector pipe = getProjector(2, 2, 30, rowReceiver);
        RowSender.generateRowsInRangeAndEmit(0, 10, pipe);
        assertThat(rowReceiver.result().size(), is(0));
    }

    @Test
    public void testInvalidNegativeLimit() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("invalid limit -1, this projector only supports positive limits");

        new SortingTopNProjector(INPUT_LITERAL_LIST, COLLECT_EXPRESSIONS, 2, FIRST_CELL_ORDERING, -1, 0);
    }

    @Test
    public void testInvalidZeroLimit() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("invalid limit 0, this projector only supports positive limits");

        new SortingTopNProjector(INPUT_LITERAL_LIST, COLLECT_EXPRESSIONS, 2, FIRST_CELL_ORDERING, 0, 0);
    }

    @Test
    public void testInvalidOffset() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("invalid offset -1");

        new SortingTopNProjector(INPUT_LITERAL_LIST, COLLECT_EXPRESSIONS, 2, FIRST_CELL_ORDERING, 1, -1);
    }
}

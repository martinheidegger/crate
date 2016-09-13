/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.operation.aggregation.impl;

import com.google.common.collect.ImmutableList;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Symbol;
import io.crate.breaker.RamAccountingContext;
import io.crate.breaker.SizeEstimator;
import io.crate.breaker.SizeEstimatorFactory;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.StmtCtx;
import io.crate.operation.Input;
import io.crate.operation.aggregation.AggregationFunction;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.SetType;
import org.elasticsearch.common.breaker.CircuitBreakingException;

import java.util.*;

public class StatSampleAggregation extends AggregationFunction<Set<Object>, Set<Object>> {

    public static final String NAME = "stat_sample";
    private final SizeEstimator<Object> innerTypeEstimator;

    private FunctionInfo info;

    private long sampleSize;

    public static void register(AggregationImplModule mod) {
        for (final DataType dataType : DataTypes.PRIMITIVE_TYPES) {
            mod.register(new StatSampleAggregation(new FunctionInfo(new FunctionIdent(NAME,
                    ImmutableList.of(dataType, DataTypes.LONG)),
                    new SetType(dataType), FunctionInfo.Type.AGGREGATE)));
        }
    }

    StatSampleAggregation(FunctionInfo info) {
        this.innerTypeEstimator = SizeEstimatorFactory.create(((SetType) info.returnType()).innerType());
        this.info = info;
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    @Override
    public Set<Object> iterate(RamAccountingContext ramAccountingContext, Set<Object> state, Input... args) throws CircuitBreakingException {
        Object value = args[0].value();
        this.sampleSize = (long) args[1].value();
        if (value == null) {
            return state;
        }
        if (state.add(value)) {
            ramAccountingContext.addBytes(innerTypeEstimator.estimateSize(value));
        }
        return state;
    }

    @Override
    public Set<Object> newState(RamAccountingContext ramAccountingContext) {
        ramAccountingContext.addBytes(36L); // overhead for the HashSet (map ref 8 + 28 for fields inside the map)
        return new HashSet<>();
    }

    @Override
    public DataType partialType() {
        return info.returnType();
    }

    @Override
    public Set<Object> reduce(RamAccountingContext ramAccountingContext, Set<Object> state1, Set<Object> state2) {
        int count = 0;

        List<Object> reservoir = new ArrayList<Object>();

        Random random = new Random();

        for (Object newValue : state2) {
            if (count < sampleSize) {
                reservoir.add(newValue);

            } else {
                int j = random.nextInt(count);
                if (j < reservoir.size()) {
                    reservoir.set(j, newValue);
                }
            }

            count++;
        }

        state1 = new HashSet<Object>(reservoir);
        return state1;
    }

    @Override
    public Set<Object> terminatePartial(RamAccountingContext ramAccountingContext, Set<Object> state) {
        return state;
    }


}

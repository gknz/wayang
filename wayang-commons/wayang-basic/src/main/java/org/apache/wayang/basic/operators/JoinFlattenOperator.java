/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.basic.operators;

import org.apache.commons.lang3.Validate;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimator;
import org.apache.wayang.core.optimizer.cardinality.DefaultCardinalityEstimator;
import org.apache.wayang.core.plan.wayangplan.UnaryToUnaryOperator;
import org.apache.wayang.core.types.DataSetType;

import java.util.Optional;

/**
 * Flattens a {@link Tuple2} of two {@link Record}s into a single {@link Record}
 * containing the concatenation of both records' fields. This operator exists to
 * bridge the type-system mismatch between {@link JoinOperator}'s output type
 * ({@code Tuple2<Record, Record>}) and downstream operators that expect a
 * single {@code Record} (such as {@code TableSink}, projections, or filters).
 *
 * <p>On platforms where data is already in flat row form (such as SQL
 * databases), this operator is a structural no-op. On platforms where data
 * is in {@link Tuple2} form (Java, Spark), it performs record concatenation
 * with collision renaming for duplicate column names ({@code left_<name>}
 * and {@code right_<name>}).
 */
public class JoinFlattenOperator
        extends UnaryToUnaryOperator<Tuple2<Record, Record>, Record> {

    private static DataSetType<Tuple2<Record, Record>> createInputDataSetType() {
        return DataSetType.createDefaultUnchecked(Tuple2.class);
    }

    private static DataSetType<Record> createOutputDataSetType() {
        return DataSetType.createDefault(Record.class);
    }

    /**
     * Creates a new instance.
     */
    public JoinFlattenOperator() {
        super(
                JoinFlattenOperator.createInputDataSetType(),
                JoinFlattenOperator.createOutputDataSetType(),
                true
        );
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public JoinFlattenOperator(JoinFlattenOperator that) {
        super(that);
    }

    @Override
    public Optional<CardinalityEstimator> createCardinalityEstimator(
            final int outputIndex,
            final Configuration configuration) {
        Validate.inclusiveBetween(0, this.getNumOutputs() - 1, outputIndex);
        // Flatten is a 1:1 transformation: every input tuple produces exactly one output record.
        return Optional.of(new DefaultCardinalityEstimator(
                1.0d, 1, this.isSupportingBroadcastInputs(),
                inputCards -> inputCards[0]
        ));
    }
}

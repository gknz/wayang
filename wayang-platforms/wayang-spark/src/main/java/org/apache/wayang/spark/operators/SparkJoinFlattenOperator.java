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

package org.apache.wayang.spark.operators;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.operators.JoinFlattenOperator;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.spark.channels.RddChannel;
import org.apache.wayang.spark.execution.SparkExecutor;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Spark implementation of the {@link JoinFlattenOperator}.
 *
 * <p>Concatenates the field arrays of the two {@link Record}s in each
 * {@link Tuple2} into a single {@link Record}. The output record's field
 * array is the left record's fields followed by the right record's fields,
 * preserving order. Downstream operators access fields by position.
 */
public class SparkJoinFlattenOperator
        extends JoinFlattenOperator
        implements SparkExecutionOperator {

    /**
     * Creates a new instance.
     */
    public SparkJoinFlattenOperator() {
        super();
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public SparkJoinFlattenOperator(JoinFlattenOperator that) {
        super(that);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            SparkExecutor sparkExecutor,
            OptimizationContext.OperatorContext operatorContext) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        final RddChannel.Instance input = (RddChannel.Instance) inputs[0];
        final RddChannel.Instance output = (RddChannel.Instance) outputs[0];

        final JavaRDD<Tuple2<Record, Record>> inputRdd = input.provideRdd();
        final JavaRDD<Record> outputRdd = inputRdd.map(new RecordFlattener());
        this.name(outputRdd);
        output.accept(outputRdd, sparkExecutor);

        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
    }

    /**
     * Concatenates the field arrays of the two records in a {@link Tuple2}
     * into a single {@link Record}.
     */
    private static class RecordFlattener implements Function<Tuple2<Record, Record>, Record> {
        @Override
        public Record call(Tuple2<Record, Record> tuple) {
            final Object[] leftValues = tuple.field0.getValues();
            final Object[] rightValues = tuple.field1.getValues();
            final Object[] combined = Arrays.copyOf(leftValues, leftValues.length + rightValues.length);
            System.arraycopy(rightValues, 0, combined, leftValues.length, rightValues.length);
            return new Record(combined);
        }
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "wayang.spark.joinflatten.load";
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new SparkJoinFlattenOperator(this);
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        assert index <= this.getNumInputs() || (index == 0 && this.getNumInputs() == 0);
        return Arrays.asList(RddChannel.UNCACHED_DESCRIPTOR, RddChannel.CACHED_DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        assert index <= this.getNumOutputs() || (index == 0 && this.getNumOutputs() == 0);
        return Collections.singletonList(RddChannel.UNCACHED_DESCRIPTOR);
    }

    @Override
    public boolean containsAction() {
        return false;
    }
}
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
package org.apache.wayang.jdbc.operators;

import org.apache.wayang.basic.operators.JoinFlattenOperator;
import org.apache.wayang.jdbc.compiler.FunctionCompiler;

import java.sql.Connection;

/**
 * JDBC implementation of the {@link JoinFlattenOperator}.
 *
 * <p>This operator is a structural no-op on the JDBC platform: SQL joins
 * already produce flat rows, so there is nothing to transform at runtime.
 * Its sole purpose is to bridge the type system between {@link
 * org.apache.wayang.jdbc.operators.JdbcJoinOperator}'s logically-typed
 * {@code Tuple2<Record, Record>} output and downstream JDBC operators that
 * expect {@code Record}, allowing in-database execution to chain joins
 * with sinks, filters, projections, and other downstream operators.
 *
 * <p>The operator generates an empty SQL clause; the executor recognizes
 * its type and skips it during SQL composition.
 */
public abstract class JdbcJoinFlattenOperator
        extends JoinFlattenOperator
        implements JdbcExecutionOperator {

    /**
     * Creates a new instance.
     */
    public JdbcJoinFlattenOperator() {
        super();
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public JdbcJoinFlattenOperator(JoinFlattenOperator that) {
        super(that);
    }

    @Override
    public String createSqlClause(Connection connection, FunctionCompiler compiler) {
        return "";
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return String.format("wayang.%s.joinflatten.load", this.getPlatform().getPlatformId());
    }
}

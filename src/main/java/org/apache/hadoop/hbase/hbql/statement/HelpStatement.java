/*
 * Copyright (c) 2009.  The Apache Software Foundation
 *
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

package org.apache.hadoop.hbase.hbql.statement;

import org.apache.hadoop.hbase.hbql.client.ExecutionResults;

public class HelpStatement extends BasicStatement implements NonConnectionStatement {

    public HelpStatement() {
        super(null);
    }

    public ExecutionResults execute() {

        final ExecutionResults retval = new ExecutionResults();

        retval.out.println(VersionStatement.usage());

        retval.out.println(CreateMappingStatement.usage());
        retval.out.println(DropMappingStatement.usage());
        retval.out.println(ShowMappingsStatement.usage());
        retval.out.println(DescribeMappingStatement.usage());

        retval.out.println(CreateTableStatement.usage());
        retval.out.println(AlterTableStatement.usage());
        retval.out.println(DisableTableStatement.usage());
        retval.out.println(EnableTableStatement.usage());
        retval.out.println(DropTableStatement.usage());
        retval.out.println(DescribeTableStatement.usage());
        retval.out.println(ShowTablesStatement.usage());

        retval.out.println(CreateIndexStatement.usage());
        retval.out.println(DropIndexForMappingStatement.usage());
        retval.out.println(DropIndexForTableStatement.usage());
        retval.out.println(DescribeIndexForMappingStatement.usage());
        retval.out.println(DescribeIndexForTableStatement.usage());

        retval.out.println(InsertStatement.usage());
        retval.out.println(DeleteStatement.usage());
        retval.out.println(SelectStatement.usage());

        retval.out.println(ImportStatement.usage());
        retval.out.println(ParseStatement.usage());
        retval.out.println(EvalStatement.usage());
        retval.out.flush();

        return retval;
    }
}
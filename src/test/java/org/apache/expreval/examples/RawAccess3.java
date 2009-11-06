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

package org.apache.expreval.examples;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.contrib.hbql.client.HBqlException;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class RawAccess3 {

    public static void main(String[] args) throws IOException, HBqlException {

        final byte[] family = Bytes.toBytes("f1");
        final byte[] col1 = Bytes.toBytes("val1");
        final byte[] col2 = Bytes.toBytes("val2");
        final byte[] col3 = Bytes.toBytes("val3");

        final HTable table = new HTable(new HBaseConfiguration(), "table1");

        final Scan scan = new Scan();
        scan.addColumn(family, col1);
        scan.addColumn(family, col2);
        scan.addColumn(family, col3);
        ResultScanner scanner = table.getScanner(scan);

        for (final Result result : scanner) {
            System.out.println(Bytes.toString(result.getRow()) + " - "
                               + Bytes.toInt(result.getValue(family, col1)) + " - "
                               + Bytes.toInt(result.getValue(family, col2)) + " - "
                               + Bytes.toInt(result.getValue(family, col3)));
        }
    }
}
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

package org.apache.hadoop.hbase.hbql.index;

import org.apache.hadoop.hbase.client.tableindexed.IndexKeyGenerator;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

public class TypedKeyGenerator implements IndexKeyGenerator {

    private byte[] columnBytes;
    private byte[] columnBytes;


    public byte[] createIndexKey(final byte[] rowKey, final Map<byte[], byte[]> columns) {

        // Extract key value for each of the indexed attribs

        return Bytes.add(columns.get(column), rowKey);
        return new byte[0];
    }

    public void write(final DataOutput dataOutput) throws IOException {

    }

    public void readFields(final DataInput dataInput) throws IOException {

    }
}

/*
 * Copyright (c) 2010.  The Apache Software Foundation
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

package org.apache.hadoop.hbase.hbql.client;

import java.util.Collection;

public class Util {

    public static String getZeroPaddedNonNegativeNumber(final long val, final int width) throws HBqlException {

        if (val < 0)
            throw new HBqlException("Value " + val + " must be non-negative");

        final String strval = "" + val;
        final int padsize = width - strval.length();
        if (padsize < 0)
            throw new HBqlException("Value " + val + " exceeds width " + width);

        final StringBuilder sbuf = new StringBuilder();
        for (int i = 0; i < padsize; i++)
            sbuf.append("0");

        sbuf.append(strval);
        return sbuf.toString();
    }

    public static byte[][] getStringsAsBytes(final Collection<String> vals) {

        final byte[][] retval;

        if (vals == null) {
            retval = null;
        }
        else {
            retval = new byte[vals.size()][];
            int cnt = 0;
            for (final String val : vals)
                retval[cnt++] = val.getBytes();
        }

        return retval;
    }

    public static byte[] getFixedWidthString(final char c, final int len) {
        final StringBuilder sbuf = new StringBuilder(len);
        for (int i = 0; i < len; i++)
            sbuf.append(c);
        return sbuf.toString().getBytes();
    }
}

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

package org.apache.hadoop.hbase.hbql.filter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Implementation of Filter interface that limits results to a specific page
 * size. It terminates scanning once the number of filter-passed rows is >
 * the given page size.
 * <p/>
 * Note that this filter cannot guarantee that the number of results returned
 * to a client are <= page size. This is because the filter is applied
 * separately on different region servers. It does however optimize the scan of
 * individual HRegions by making sure that the page size is never exceeded
 * locally.
 */
public class PageFilter implements InstrumentedFilter {

    private static final Log LOG = LogFactory.getLog(PageFilter.class);

    private boolean verbose = false;
    private long pageSize = Long.MAX_VALUE;
    private int rowsAccepted = 0;
    private Filter filter;

    /**
     * Default constructor, filters nothing. Required though for RPC
     * deserialization.
     */
    public PageFilter() {
        super();
    }

    /**
     * Constructor that takes a maximum page size.
     *
     * @param pageSize Maximum result size.
     */
    public PageFilter(final long pageSize, final Filter filter) {
        this.pageSize = pageSize;
        this.filter = filter;
    }

    private Filter getFilter() {
        return this.filter;
    }

    public void setVerbose(final boolean verbose) {
        this.verbose = verbose;
    }

    public boolean getVerbose() {
        return this.verbose;
    }

    public void reset() {
        if (this.getFilter() != null)
            this.getFilter().reset();
    }

    public boolean filterAllRemaining() {
        if (this.getVerbose())
            LOG.debug("In PageFilter.filterAllRemaining() " + (this.rowsAccepted >= this.pageSize)
                      + " " + this.rowsAccepted + " of " + this.pageSize);
        return this.rowsAccepted >= this.pageSize;
    }

    public boolean filterRowKey(byte[] rowKey, int offset, int length) {
        if (this.getFilter() != null)
            return this.getFilter().filterRowKey(rowKey, offset, length);
        else
            return false;
    }

    public ReturnCode filterKeyValue(KeyValue v) {
        if (this.getFilter() != null)
            return this.getFilter().filterKeyValue(v);
        else
            return ReturnCode.INCLUDE;
    }

    public boolean filterRow() {

        if (this.rowsAccepted > this.pageSize)
            return true;

        if (this.getFilter() != null) {
            if (this.getFilter().filterRow())
                return true;
        }

        this.rowsAccepted++;
        return this.rowsAccepted > this.pageSize;
    }

    public void readFields(final DataInput in) throws IOException {
        Configuration conf = new HBaseConfiguration();
        this.pageSize = in.readLong();
        this.verbose = in.readBoolean();
        this.filter = (Filter)HbaseObjectWritable.readObject(in, conf);
    }

    public void write(final DataOutput out) throws IOException {
        Configuration conf = new HBaseConfiguration();
        out.writeLong(this.pageSize);
        out.writeBoolean(this.getVerbose());
        HbaseObjectWritable.writeObject(out, this.getFilter(), Writable.class, conf);
    }
}
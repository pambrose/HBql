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

package org.apache.hadoop.hbase.hbql.schema;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.schema.property.BlockCacheProperty;
import org.apache.hadoop.hbase.hbql.schema.property.BlockSizeProperty;
import org.apache.hadoop.hbase.hbql.schema.property.BloomFilterProperty;
import org.apache.hadoop.hbase.hbql.schema.property.CompressionTypeProperty;
import org.apache.hadoop.hbase.hbql.schema.property.FamilyProperty;
import org.apache.hadoop.hbase.hbql.schema.property.InMemoryProperty;
import org.apache.hadoop.hbase.hbql.schema.property.IndexIntervalProperty;
import org.apache.hadoop.hbase.hbql.schema.property.MaxVersionsProperty;
import org.apache.hadoop.hbase.hbql.schema.property.TtlProperty;

import java.util.List;

public class FamilyDefinition {

    private final String familyName;
    private final List<FamilyProperty> familyPropertyList;

    private BlockCacheProperty blockCache = null;
    private BlockSizeProperty blockSize = null;
    private BloomFilterProperty bloomFilter = null;
    private CompressionTypeProperty compressionType = null;
    private IndexIntervalProperty indexInterval = null;
    private InMemoryProperty inMemory = null;
    private MaxVersionsProperty maxVersions = null;
    private TtlProperty ttl = null;


    public FamilyDefinition(final String familyName, final List<FamilyProperty> familyPropertyList) {
        this.familyName = familyName;
        this.familyPropertyList = familyPropertyList;
    }

    public String getFamilyName() {
        return familyName;
    }

    public HColumnDescriptor getColumnDescriptor() throws HBqlException {

        this.validateFamily();

        return new HColumnDescriptor(this.getFamilyName().getBytes(),
                                     this.maxVersions.getValue(),
                                     this.compressionType.getValue().name(),
                                     this.inMemory.getValue(),
                                     this.blockCache.getValue(),
                                     this.ttl.getValue(),
                                     this.bloomFilter.getValue());
    }

    private void validateProperty(final FamilyProperty familyProperty, final String str) throws HBqlException {
        if (familyProperty != null)
            throw new HBqlException("Multiple " + str + " values for " + this.getFamilyName() + " not allowed");
    }

    private void validateFamily() throws HBqlException {

        for (final FamilyProperty familyProperty : this.familyPropertyList) {

            switch (familyProperty.getPropertyType()) {

                case BLOCKCACHE: {
                    this.validateProperty(this.blockCache, "BLOCK CACHE");
                    this.blockCache = (BlockCacheProperty)familyProperty;
                    break;
                }

                case BLOCKSIZE: {
                    this.validateProperty(this.blockSize, "BLOCK SIZE");
                    this.blockSize = (BlockSizeProperty)familyProperty;
                    break;
                }

                case BLOOMFILTER: {
                    this.validateProperty(this.bloomFilter, "BLOOM FILTER");
                    this.bloomFilter = (BloomFilterProperty)familyProperty;
                    break;
                }

                case COMPRESSIONTYPE: {
                    this.validateProperty(this.compressionType, "COMPRESSION TYPE");
                    this.compressionType = (CompressionTypeProperty)familyProperty;
                    break;
                }

                case INDEXINTERVAL: {
                    this.validateProperty(this.indexInterval, "INDEX INTERVAL");
                    this.indexInterval = (IndexIntervalProperty)familyProperty;
                    break;
                }

                case INMEMORY: {
                    this.validateProperty(this.inMemory, "IN MEMORY");
                    this.inMemory = (InMemoryProperty)familyProperty;
                    break;
                }

                case MAXVERSIONS: {
                    this.validateProperty(this.maxVersions, "MAX VERSIONS");
                    this.maxVersions = (MaxVersionsProperty)familyProperty;
                    break;
                }

                case TTL: {
                    this.validateProperty(this.ttl, "TTL");
                    this.ttl = (TtlProperty)familyProperty;
                    break;
                }
            }
        }
    }
}

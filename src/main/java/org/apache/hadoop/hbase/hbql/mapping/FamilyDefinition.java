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

package org.apache.hadoop.hbase.hbql.mapping;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.mapping.property.BlockCacheProperty;
import org.apache.hadoop.hbase.hbql.mapping.property.BlockSizeProperty;
import org.apache.hadoop.hbase.hbql.mapping.property.BloomFilterProperty;
import org.apache.hadoop.hbase.hbql.mapping.property.CompressionTypeProperty;
import org.apache.hadoop.hbase.hbql.mapping.property.FamilyProperty;
import org.apache.hadoop.hbase.hbql.mapping.property.InMemoryProperty;
import org.apache.hadoop.hbase.hbql.mapping.property.MapFileIndexIntervalProperty;
import org.apache.hadoop.hbase.hbql.mapping.property.MaxVersionsProperty;
import org.apache.hadoop.hbase.hbql.mapping.property.TtlProperty;

import java.util.List;

public class FamilyDefinition {

    private final String familyName;
    private final List<FamilyProperty> familyPropertyList;

    private BlockCacheProperty blockCache = null;
    private BlockSizeProperty blockSize = null;
    private BloomFilterProperty bloomFilter = null;
    private CompressionTypeProperty compressionType = null;
    private MapFileIndexIntervalProperty mapFileIndexInterval = null;
    private InMemoryProperty inMemory = null;
    private MaxVersionsProperty maxVersions = null;
    private TtlProperty ttl = null;

    public FamilyDefinition(final String familyName, final List<FamilyProperty> familyPropertyList) {
        this.familyName = familyName;
        this.familyPropertyList = familyPropertyList;
    }

    public String getFamilyName() {
        return this.familyName;
    }

    private List<FamilyProperty> getFamilyPropertyList() {
        return this.familyPropertyList;
    }

    public HColumnDescriptor getHColumnDescriptor() throws HBqlException {

        this.validateFamilyPropertyList();

        //final String name = (this.getFamilyName().endsWith(":")) ? this.getFamilyName() : this.getFamilyName() + ":";
        final HColumnDescriptor columnDesc = new HColumnDescriptor(this.getFamilyName());

        if (this.maxVersions != null)
            columnDesc.setMaxVersions(this.maxVersions.getValue());
        if (this.compressionType != null)
            columnDesc.setCompressionType(this.compressionType.getValue());
        if (this.inMemory != null)
            columnDesc.setInMemory(this.inMemory.getValue());
        if (this.blockCache != null)
            columnDesc.setBlockCacheEnabled(this.blockCache.getValue());
        if (this.blockSize != null)
            columnDesc.setBlocksize(this.blockSize.getValue());
        if (this.mapFileIndexInterval != null)
            columnDesc.setMapFileIndexInterval(this.mapFileIndexInterval.getValue());
        if (this.ttl != null)
            columnDesc.setTimeToLive(this.ttl.getValue());
        if (this.bloomFilter != null)
            columnDesc.setBloomfilter(this.bloomFilter.getValue());

        return columnDesc;
    }

    private FamilyProperty validateProperty(final FamilyProperty assignee,
                                            final FamilyProperty value) throws HBqlException {
        if (assignee != null)
            throw new HBqlException("Multiple " + value.getPropertyType().getDescription()
                                    + " values for " + this.getFamilyName() + " not allowed");
        return value;
    }

    private void validateFamilyPropertyList() throws HBqlException {

        if (this.getFamilyPropertyList() == null)
            return;

        for (final FamilyProperty familyProperty : this.getFamilyPropertyList()) {

            switch (familyProperty.getPropertyType()) {

                case BLOCKCACHE:
                    this.blockCache = (BlockCacheProperty)this.validateProperty(this.blockCache, familyProperty);
                    break;

                case BLOCKSIZE:
                    this.blockSize = (BlockSizeProperty)this.validateProperty(this.blockSize, familyProperty);
                    break;

                case BLOOMFILTER:
                    this.bloomFilter = (BloomFilterProperty)this.validateProperty(this.bloomFilter, familyProperty);
                    break;

                case COMPRESSIONTYPE:
                    this.compressionType = (CompressionTypeProperty)this.validateProperty(this.compressionType, familyProperty);
                    break;

                case MAPFILEINDEXINTERVAL:
                    this.mapFileIndexInterval = (MapFileIndexIntervalProperty)this.validateProperty(this.mapFileIndexInterval, familyProperty);
                    break;

                case INMEMORY:
                    this.inMemory = (InMemoryProperty)this.validateProperty(this.inMemory, familyProperty);
                    break;

                case MAXVERSIONS:
                    this.maxVersions = (MaxVersionsProperty)this.validateProperty(this.maxVersions, familyProperty);
                    break;

                case TTL:
                    this.ttl = (TtlProperty)this.validateProperty(this.ttl, familyProperty);
                    break;
            }
        }
    }
}

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

import java.util.List;

public class FamilyDefinition {

    private final String familyName;
    private final List<FamilyProperty> mappingPropertyList;

    private FamilyProperty maxVersions = null;
    private FamilyProperty mapFileIndexInterval = null;
    private FamilyProperty ttl = null;
    private FamilyProperty blockSize = null;
    private FamilyProperty blockCacheEnabled = null;
    private FamilyProperty bloomFilterEnabled = null;
    private FamilyProperty inMemoryEnabled = null;
    private CompressionTypeProperty compressionType = null;

    public FamilyDefinition(final String familyName, final List<FamilyProperty> mappingPropertyList) {
        this.familyName = familyName;
        this.mappingPropertyList = mappingPropertyList;
    }

    public String getFamilyName() {
        return this.familyName;
    }

    private List<FamilyProperty> getFamilyPropertyList() {
        return this.mappingPropertyList;
    }

    public HColumnDescriptor getHColumnDescriptor() throws HBqlException {

        this.validateFamilyPropertyList();

        final HColumnDescriptor columnDesc = new HColumnDescriptor(this.getFamilyName());

        if (this.maxVersions != null)
            columnDesc.setMaxVersions(this.maxVersions.getIntegerValue());
        if (this.mapFileIndexInterval != null)
            columnDesc.setMapFileIndexInterval(this.mapFileIndexInterval.getIntegerValue());
        if (this.ttl != null)
            columnDesc.setTimeToLive(this.ttl.getIntegerValue());
        if (this.blockSize != null)
            columnDesc.setBlocksize(this.blockSize.getIntegerValue());
        if (this.blockCacheEnabled != null)
            columnDesc.setBlockCacheEnabled(this.blockCacheEnabled.getBooleanValue());
        if (this.inMemoryEnabled != null)
            columnDesc.setInMemory(this.inMemoryEnabled.getBooleanValue());
        if (this.bloomFilterEnabled != null)
            columnDesc.setBloomfilter(this.bloomFilterEnabled.getBooleanValue());
        if (this.compressionType != null)
            columnDesc.setCompressionType(this.compressionType.getValue());

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

        for (final FamilyProperty mappingProperty : this.getFamilyPropertyList()) {

            mappingProperty.validate();

            switch (mappingProperty.getEnumType()) {

                case MAX_VERSIONS:
                    this.maxVersions = this.validateProperty(this.maxVersions, mappingProperty);
                    break;

                case MAP_FILE_INDEX_INTERVAL:
                    this.mapFileIndexInterval = this.validateProperty(this.mapFileIndexInterval, mappingProperty);
                    break;

                case TTL:
                    this.ttl = this.validateProperty(this.ttl, mappingProperty);
                    break;

                case BLOCK_SIZE:
                    this.blockSize = this.validateProperty(this.blockSize, mappingProperty);
                    break;

                case BLOCK_CACHE_ENABLED:
                    this.blockCacheEnabled = this.validateProperty(this.blockCacheEnabled, mappingProperty);
                    break;

                case IN_MEMORY:
                    this.inMemoryEnabled = this.validateProperty(this.inMemoryEnabled, mappingProperty);
                    break;

                case BLOOM_FILTER:
                    this.bloomFilterEnabled = this.validateProperty(this.bloomFilterEnabled, mappingProperty);
                    break;

                case COMPRESSION_TYPE:
                    this.compressionType = (CompressionTypeProperty)this.validateProperty(this.compressionType, mappingProperty);
                    break;
            }
        }
    }
}

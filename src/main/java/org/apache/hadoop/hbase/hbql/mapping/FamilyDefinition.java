/*
 * Copyright (c) 2011.  The Apache Software Foundation
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

    private final String               familyName;
    private final List<FamilyProperty> familyPropertyList;

    private FamilyProperty          maxVersions       = null;
    private FamilyProperty          ttl               = null;
    private FamilyProperty          blockSize         = null;
    private FamilyProperty          blockCacheEnabled = null;
    private FamilyProperty          inMemoryEnabled   = null;
    private BloomFilterTypeProperty bloomFilterType   = null;
    private CompressionTypeProperty compressionType   = null;

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

    public HColumnDescriptor getColumnDescription() throws HBqlException {

        this.validateFamilyPropertyList();

        final HColumnDescriptor columnDescriptor = new HColumnDescriptor(this.getFamilyName());
        this.assignFamilyProperties(columnDescriptor);
        return columnDescriptor;
    }

    private void assignFamilyProperties(final HColumnDescriptor columnDesc) throws HBqlException {

        if (this.maxVersions != null)
            columnDesc.setMaxVersions(this.maxVersions.getIntegerValue());
        if (this.ttl != null)
            columnDesc.setTimeToLive(this.ttl.getIntegerValue());
        if (this.blockSize != null)
            columnDesc.setBlocksize(this.blockSize.getIntegerValue());
        if (this.blockCacheEnabled != null)
            columnDesc.setBlockCacheEnabled(this.blockCacheEnabled.getBooleanValue());
        if (this.inMemoryEnabled != null)
            columnDesc.setInMemory(this.inMemoryEnabled.getBooleanValue());
        if (this.bloomFilterType != null)
            columnDesc.setBloomFilterType(this.bloomFilterType.getBloomValue());
        if (this.compressionType != null)
            columnDesc.setCompressionType(this.compressionType.getCompressionValue());
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

            familyProperty.validate();

            switch (familyProperty.getEnumType()) {

                case MAX_VERSIONS:
                    this.maxVersions = this.validateProperty(this.maxVersions, familyProperty);
                    break;

                case TTL:
                    this.ttl = this.validateProperty(this.ttl, familyProperty);
                    break;

                case BLOCK_SIZE:
                    this.blockSize = this.validateProperty(this.blockSize, familyProperty);
                    break;

                case BLOCK_CACHE_ENABLED:
                    this.blockCacheEnabled = this.validateProperty(this.blockCacheEnabled, familyProperty);
                    break;

                case IN_MEMORY:
                    this.inMemoryEnabled = this.validateProperty(this.inMemoryEnabled, familyProperty);
                    break;

                case BLOOMFILTER_TYPE:
                    this.bloomFilterType = (BloomFilterTypeProperty)this
                            .validateProperty(this.bloomFilterType, familyProperty);
                    break;

                case COMPRESSION_TYPE:
                    this.compressionType = (CompressionTypeProperty)this
                            .validateProperty(this.compressionType, familyProperty);
                    break;
            }
        }
    }
}

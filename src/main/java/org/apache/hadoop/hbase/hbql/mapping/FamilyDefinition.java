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

    private IntegerFamilyProperty maxVersions = null;
    private IntegerFamilyProperty mapFileIndexInterval = null;
    private IntegerFamilyProperty ttl = null;
    private IntegerFamilyProperty blockSize = null;
    private BooleanFamilyProperty blockCacheEnabled = null;
    private BooleanFamilyProperty bloomFilterEnabled = null;
    private BooleanFamilyProperty inMemoryEnabled = null;
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
            columnDesc.setMaxVersions(this.maxVersions.getValue());
        if (this.mapFileIndexInterval != null)
            columnDesc.setMapFileIndexInterval(this.mapFileIndexInterval.getValue());
        if (this.ttl != null)
            columnDesc.setTimeToLive(this.ttl.getValue());
        if (this.blockSize != null)
            columnDesc.setBlocksize(this.blockSize.getValue());
        if (this.blockCacheEnabled != null)
            columnDesc.setBlockCacheEnabled(this.blockCacheEnabled.getValue());
        if (this.inMemoryEnabled != null)
            columnDesc.setInMemory(this.inMemoryEnabled.getValue());
        if (this.bloomFilterEnabled != null)
            columnDesc.setBloomfilter(this.bloomFilterEnabled.getValue());
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

                case MAXVERSIONS:
                    this.maxVersions = (IntegerFamilyProperty)this.validateProperty(this.maxVersions, mappingProperty);
                    break;

                case MAPFILEINDEXINTERVAL:
                    this.mapFileIndexInterval = (IntegerFamilyProperty)this.validateProperty(this.mapFileIndexInterval, mappingProperty);
                    break;

                case TTL:
                    this.ttl = (IntegerFamilyProperty)this.validateProperty(this.ttl, mappingProperty);
                    break;

                case BLOCKSIZE:
                    this.blockSize = (IntegerFamilyProperty)this.validateProperty(this.blockSize, mappingProperty);
                    break;

                case BLOCKCACHEENABLED:
                    this.blockCacheEnabled = (BooleanFamilyProperty)this.validateProperty(this.blockCacheEnabled, mappingProperty);
                    break;

                case INMEMORY:
                    this.inMemoryEnabled = (BooleanFamilyProperty)this.validateProperty(this.inMemoryEnabled, mappingProperty);
                    break;

                case BLOOMFILTER:
                    this.bloomFilterEnabled = (BooleanFamilyProperty)this.validateProperty(this.bloomFilterEnabled, mappingProperty);
                    break;

                case COMPRESSIONTYPE:
                    this.compressionType = (CompressionTypeProperty)this.validateProperty(this.compressionType, mappingProperty);
                    break;
            }
        }
    }
}

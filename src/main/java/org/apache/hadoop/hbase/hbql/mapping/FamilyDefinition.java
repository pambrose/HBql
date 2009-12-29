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
import org.apache.hadoop.hbase.hbql.mapping.property.BooleanProperty;
import org.apache.hadoop.hbase.hbql.mapping.property.CompressionTypeProperty;
import org.apache.hadoop.hbase.hbql.mapping.property.FamilyProperty;
import org.apache.hadoop.hbase.hbql.mapping.property.IntegerProperty;

import java.util.List;

public class FamilyDefinition {

    private final String familyName;
    private final List<FamilyProperty> mappingPropertyList;

    private IntegerProperty maxVersions = null;
    private CompressionTypeProperty compressionType = null;
    private BooleanProperty inMemory = null;
    private BooleanProperty blockCache = null;
    private IntegerProperty blockSize = null;
    private IntegerProperty mapFileIndexInterval = null;
    private IntegerProperty ttl = null;
    private BooleanProperty bloomFilter = null;

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

        for (final FamilyProperty mappingProperty : this.getFamilyPropertyList()) {

            mappingProperty.validate();

            switch (mappingProperty.getPropertyType()) {

                case MAXVERSIONS:
                    this.maxVersions = (IntegerProperty)this.validateProperty(this.maxVersions, mappingProperty);
                    break;

                case COMPRESSIONTYPE:
                    this.compressionType = (CompressionTypeProperty)this.validateProperty(this.compressionType, mappingProperty);
                    break;

                case INMEMORY:
                    this.inMemory = (BooleanProperty)this.validateProperty(this.inMemory, mappingProperty);
                    break;

                case BLOCKCACHE:
                    this.blockCache = (BooleanProperty)this.validateProperty(this.blockCache, mappingProperty);
                    break;

                case BLOCKSIZE:
                    this.blockSize = (IntegerProperty)this.validateProperty(this.blockSize, mappingProperty);
                    break;

                case MAPFILEINDEXINTERVAL:
                    this.mapFileIndexInterval = (IntegerProperty)this.validateProperty(this.mapFileIndexInterval, mappingProperty);
                    break;

                case TTL:
                    this.ttl = (IntegerProperty)this.validateProperty(this.ttl, mappingProperty);
                    break;

                case BLOOMFILTER:
                    this.bloomFilter = (BooleanProperty)this.validateProperty(this.bloomFilter, mappingProperty);
                    break;
            }
        }
    }
}

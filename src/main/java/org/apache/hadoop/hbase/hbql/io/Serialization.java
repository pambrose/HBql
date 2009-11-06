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

package org.apache.hadoop.hbase.hbql.io;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.schema.FieldType;

public abstract class Serialization {

    public enum TYPE {
        JAVA, HADOOP
    }

    private final static Serialization java = new JavaSerialization();
    private final static Serialization hadoop = new HadoopSerialization();

    public static Serialization getSerializationStrategy(final TYPE type) {

        switch (type) {
            case JAVA:
                return java;
            case HADOOP:
                return hadoop;
        }

        return null;
    }

    abstract public Object getScalarFromBytes(FieldType fieldType, byte[] b) throws HBqlException;

    abstract public byte[] getScalarAsBytes(FieldType fieldType, Object obj) throws HBqlException;

    abstract public Object getArrayFromBytes(FieldType fieldType, Class clazz, byte[] b) throws HBqlException;

    abstract public byte[] getArrayAsBytes(FieldType fieldType, Object obj) throws HBqlException;

    public byte[] getStringAsBytes(final String obj) throws HBqlException {
        return this.getScalarAsBytes(FieldType.StringType, obj);
    }

    public byte[] getScalarAsBytes(final Object obj) throws HBqlException {
        return this.getScalarAsBytes(FieldType.getFieldType(obj), obj);
    }

    public String getStringFromBytes(final byte[] b) throws HBqlException {
        return (String)this.getScalarFromBytes(FieldType.StringType, b);
    }

    public boolean isSerializable(final Object obj) {

        try {
            final byte[] b = getScalarAsBytes(obj);
            final Object newobj = getScalarFromBytes(FieldType.getFieldType(obj), b);
        }
        catch (HBqlException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    protected String getExceptionMessage(final String name, final Exception e) {
        return "Error in " + name + ": " + e.getClass().getSimpleName() + "-" + e.getMessage();
    }
}

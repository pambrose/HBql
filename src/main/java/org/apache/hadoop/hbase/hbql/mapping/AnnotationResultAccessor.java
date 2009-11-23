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

import org.apache.expreval.util.Maps;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.hbql.client.Column;
import org.apache.hadoop.hbase.hbql.client.ColumnVersionMap;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.Mapping;
import org.apache.hadoop.hbase.hbql.impl.HConnectionImpl;
import org.apache.hadoop.hbase.hbql.statement.NonStatement;
import org.apache.hadoop.hbase.hbql.statement.StatementContext;
import org.apache.hadoop.hbase.hbql.statement.select.SelectElement;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

public class AnnotationResultAccessor extends ResultAccessor {


    private final Class<?> clazz;
    private final Map<String, CurrentValueAnnotationAttrib> columnMap = Maps.newHashMap();
    private final Map<String, VersionAnnotationAttrib> columnVersionMap = Maps.newHashMap();

    private AnnotationResultAccessor(final HBaseTableMapping tableMapping, final Class clazz) throws HBqlException {

        super(new NonStatement(tableMapping, null));

        this.getStatementContext().setResultAccessor(this);

        this.clazz = clazz;

        // Make sure there is an empty constructor declared
        try {
            this.getClazz().getConstructor();
        }
        catch (NoSuchMethodException e) {
            throw new HBqlException("Class " + this + " is missing a null constructor");
        }

        for (final Field field : clazz.getDeclaredFields()) {
            if (field.getAnnotation(Column.class) != null)
                this.processColumnAnnotation(field);

            if (field.getAnnotation(ColumnVersionMap.class) != null)
                this.processColumnVersionAnnotation(field);
        }

        if (!this.getColumnMap().containsKey(this.getMapping().getKeyAttrib().getFamilyQualifiedName()))
            throw new HBqlException(this.getClazz().getName() + " must contain a mapping to key attribute "
                                    + this.getMapping().getKeyAttrib().getFamilyQualifiedName());
    }

    public static boolean isAnnotatedObject(final Class<?> clazz) {
        return clazz.getAnnotation(org.apache.hadoop.hbase.hbql.client.Mapping.class) != null;
    }

    public static AnnotationResultAccessor newAnnotationMapping(final HConnectionImpl connection,
                                                                final Class<?> clazz) throws HBqlException {

        Mapping mappingAnnotation = clazz.getAnnotation(Mapping.class);

        if (mappingAnnotation == null)
            throw new HBqlException("Class " + clazz.getName() + " is missing @Mapping annotation");

        if (mappingAnnotation.name() == null || mappingAnnotation.name().length() == 0)
            throw new HBqlException("@Mapping annotation for class " + clazz.getName() + " is missing a name");

        HBaseTableMapping tableMapping = connection.getMapping(mappingAnnotation.name());
        return new AnnotationResultAccessor(tableMapping, clazz);
    }

    private void processColumnAnnotation(final Field field) throws HBqlException {

        final Column columnAnno = field.getAnnotation(Column.class);
        final String attribName = columnAnno.name().length() == 0 ? field.getName() : columnAnno.name();
        final HRecordAttrib columnAttrib = (HRecordAttrib)this.getMapping().getAttribByVariableName(attribName);

        if (columnAttrib == null)
            throw new HBqlException("Unknown attribute " + this.getMapping() + "." + attribName
                                    + " in " + this.getClazz().getName());

        if (this.getColumnMap().containsKey(columnAttrib.getFamilyQualifiedName()))
            throw new HBqlException("Cannot map multiple instance variables in " + this.getClazz().getName()
                                    + " to " + columnAttrib.getFamilyQualifiedName());

        final CurrentValueAnnotationAttrib attrib = new CurrentValueAnnotationAttrib(field, columnAttrib);
        this.getColumnMap().put(columnAttrib.getFamilyQualifiedName(), attrib);
    }

    private void processColumnVersionAnnotation(final Field field) throws HBqlException {

        final ColumnVersionMap versionAnno = field.getAnnotation(ColumnVersionMap.class);
        final String attribName = versionAnno.name().length() == 0 ? field.getName() : versionAnno.name();
        final ColumnAttrib columnAttrib = this.getMapping().getAttribByVariableName(attribName);

        this.getColumnVersionMap().put(columnAttrib.getFamilyQualifiedName(),
                                       new VersionAnnotationAttrib(columnAttrib.getFamilyName(),
                                                                   columnAttrib.getColumnName(),
                                                                   field,
                                                                   columnAttrib.getFieldType(),
                                                                   columnAttrib.isUnMappedAttrib(),
                                                                   columnAttrib.getGetter(),
                                                                   columnAttrib.getSetter()));
    }

    public ColumnAttrib getKeyAttrib() throws HBqlException {
        final String valname = this.getMapping().getKeyAttrib().getFamilyQualifiedName();
        return this.getAttrib(valname);
    }

    public ColumnAttrib getColumnAttribByQualifiedName(final String familyName,
                                                       final String columnName) throws HBqlException {
        final ColumnAttrib attrib = this.getHBaseTableMapping().getAttribFromFamilyQualifiedName(familyName
                                                                                                 + ":" + columnName);
        return this.getColumnAttribByName(attrib.getFamilyQualifiedName());
    }

    public ColumnAttrib getColumnAttribByName(final String name) throws HBqlException {
        final String valname = this.getMapping().getAttribByVariableName(name).getFamilyQualifiedName();
        return this.getAttrib(valname);
    }

    private Class<?> getClazz() {
        return this.clazz;
    }

    private Map<String, CurrentValueAnnotationAttrib> getColumnMap() {
        return this.columnMap;
    }

    private Map<String, VersionAnnotationAttrib> getColumnVersionMap() {
        return this.columnVersionMap;
    }

    public CurrentValueAnnotationAttrib getAttrib(final String name) {
        return this.getColumnMap().get(name);
    }

    public VersionAnnotationAttrib getVersionAttrib(final String name) {
        return this.getColumnVersionMap().get(name);
    }

    private Object newInstance() throws IllegalAccessException, InstantiationException {
        return this.getClazz().newInstance();
    }

    public Object newObject(final StatementContext statementContext,
                            final List<SelectElement> selectElementList,
                            final int maxVersions,
                            final Result result) throws HBqlException {

        try {
            // Create object and assign values
            final Object newobj = this.createNewObject();
            this.assignSelectValues(newobj, selectElementList, maxVersions, result);
            return newobj;
        }
        catch (Exception e) {
            e.printStackTrace();
            throw new HBqlException("Error in newObject() " + e.getMessage());
        }
    }

    private void assignSelectValues(final Object newobj,
                                    final List<SelectElement> selectElementList,
                                    final int maxVersions,
                                    final Result result) throws HBqlException {

        // Set key value
        this.getAttrib(this.getKeyAttrib().getFamilyQualifiedName()).setCurrentValue(newobj, 0, result.getRow());

        // Set the non-key values
        for (final SelectElement selectElement : selectElementList)
            selectElement.assignSelectValue(newobj, maxVersions, result);
    }

    private Object createNewObject() throws HBqlException {

        // Create new instance
        final Object newobj;
        try {
            newobj = this.newInstance();
        }
        catch (Exception e) {
            e.printStackTrace();
            throw new HBqlException("Cannot create new instance of " + this.getClazz().getName());
        }

        return newobj;
    }
}

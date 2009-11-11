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

import org.apache.expreval.util.Lists;
import org.apache.expreval.util.Maps;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.hbql.client.Column;
import org.apache.hadoop.hbase.hbql.client.ColumnVersionMap;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.SchemaManager;
import org.apache.hadoop.hbase.hbql.statement.select.SelectElement;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

public class AnnotationMapping extends HBaseSchema {

    private final static Map<Class<?>, AnnotationMapping> annotationMappingMap = Maps.newHashMap();
    private final static Map<String, Class<?>> classCacheMap = Maps.newHashMap();

    private final HBaseSchema mappedSchema;
    private final Class<?> clazz;
    private final Map<String, CurrentValueAnnotationAttrib> columnMap = Maps.newHashMap();
    private final Map<String, VersionAnnotationAttrib> columnVersionMap = Maps.newHashMap();

    private AnnotationMapping(final HBaseSchema mappedSchema,
                              final String schemaName,
                              final Class clazz) throws HBqlException {

        super(schemaName, mappedSchema.getTableName());

        this.mappedSchema = mappedSchema;
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

        if (!this.getColumnMap().containsKey(this.getMappedSchema().getKeyAttrib().getFamilyQualifiedName()))
            throw new HBqlException(this.getClazz().getName() + " must contain a mapping to key attribute "
                                    + this.getMappedSchema().getKeyAttrib().getFamilyQualifiedName());
    }

    public synchronized static AnnotationMapping getAnnotationMapping(final Class<?> clazz) throws HBqlException {

        AnnotationMapping mapping = getAnnotationMappingMap().get(clazz);
        if (mapping != null)
            return mapping;

        org.apache.hadoop.hbase.hbql.client.Schema schemaAnnotation =
                clazz.getAnnotation(org.apache.hadoop.hbase.hbql.client.Schema.class);

        if (schemaAnnotation == null)
            throw new HBqlException("Class " + clazz.getName() + " is missing @Schema annotation");

        if (schemaAnnotation.name() == null || schemaAnnotation.name().length() == 0)
            throw new HBqlException("@Schema annotation for class " + clazz.getName() + " is missing a name");

        final HBaseSchema schema = SchemaManager.getSchema(schemaAnnotation.name());

        mapping = new AnnotationMapping(schema, schemaAnnotation.name(), clazz);
        getAnnotationMappingMap().put(clazz, mapping);
        return mapping;
    }


    private void processColumnAnnotation(final Field field) throws HBqlException {

        final Column columnAnno = field.getAnnotation(Column.class);
        final String attribName = columnAnno.name().length() == 0 ? field.getName() : columnAnno.name();
        final HBaseAttrib columnAttrib = (HBaseAttrib)this.getMappedSchema().getAttribByVariableName(attribName);

        if (columnAttrib == null)
            throw new HBqlException("Unknown attribute " + this.getMappedSchema() + "." + attribName
                                    + " in " + this.getClazz().getName());

        if (this.getColumnMap().containsKey(columnAttrib.getFamilyQualifiedName()))
            throw new HBqlException("Cannot map multiple instance variables in " + this.getClazz().getName()
                                    + " to " + columnAttrib.getFamilyQualifiedName());

        final CurrentValueAnnotationAttrib attrib = new CurrentValueAnnotationAttrib(field, columnAttrib);
        this.getColumnMap().put(columnAttrib.getFamilyQualifiedName(), attrib);

        this.addAttribToVariableNameMap(attrib, columnAttrib.getNamesForColumn());
        this.addAttribToFamilyQualifiedNameMap(attrib);
        this.addVersionAttrib(attrib);
        this.addFamilyDefaultAttrib(attrib);
        this.addAttribToFamilyNameColumnListMap(attrib);
    }

    private void processColumnVersionAnnotation(final Field field) throws HBqlException {

        final ColumnVersionMap versionAnno = field.getAnnotation(ColumnVersionMap.class);
        final String attribName = versionAnno.name().length() == 0 ? field.getName() : versionAnno.name();
        final ColumnAttrib columnAttrib = this.getMappedSchema().getAttribByVariableName(attribName);

        this.getColumnVersionMap().put(columnAttrib.getFamilyQualifiedName(),
                                       new VersionAnnotationAttrib(columnAttrib.getFamilyName(),
                                                                   columnAttrib.getColumnName(),
                                                                   field,
                                                                   columnAttrib.getFieldType(),
                                                                   columnAttrib.isFamilyDefaultAttrib(),
                                                                   columnAttrib.getGetter(),
                                                                   columnAttrib.getSetter()));
    }

    public String getTableName() {
        return this.getMappedSchema().getTableName();
    }

    private HBaseSchema getMappedSchema() {
        return this.mappedSchema;
    }

    public ColumnAttrib getKeyAttrib() {
        final String valname = this.getMappedSchema().getKeyAttrib().getFamilyQualifiedName();
        return this.getAttrib(valname);
    }

    public ColumnAttrib getAttribByVariableName(final String name) {
        final String valname = super.getAttribByVariableName(name).getFamilyQualifiedName();
        return this.getAttrib(valname);
    }

    public synchronized static AnnotationMapping getAnnotationMapping(final String objName) throws HBqlException {

        // First see if already cached
        Class<?> clazz = getClassCacheMap().get(objName);

        if (clazz != null)
            return getAnnotationMapping(clazz);

        final String classpath = System.getProperty("java.class.path");
        for (final String val : classpath.split(":")) {
            try {
                if (val.toLowerCase().endsWith(".jar")) {
                    JarFile jarFile = new JarFile(val);
                    Enumeration<JarEntry> entries = jarFile.entries();
                    while (entries.hasMoreElements()) {
                        final JarEntry entry = entries.nextElement();
                        final AnnotationMapping mapping = searchPackage(entry.getName(), objName);
                        if (mapping != null)
                            return mapping;
                    }
                }

                final File rootdir = new File(val);
                if (rootdir.isDirectory())
                    return searchDirectory(rootdir, rootdir.getCanonicalPath(), "", objName + ".class");
            }
            catch (IOException e) {
                // Go to next entry
            }
        }

        return null;
    }

    private static AnnotationMapping searchDirectory(final File dir,
                                                     final String rootDir,
                                                     final String prefix,
                                                     final String dotClassName) throws HBqlException {

        final List<File> subdirs = Lists.newArrayList();
        final String[] contents = dir.list();

        for (final String elem : contents) {

            final File file = new File(dir + "/" + elem);
            if (file.isDirectory()) {
                subdirs.add(file);
            }
            else {
                final String pathname = file.getAbsolutePath().replace('/', '.');
                if (pathname.endsWith(dotClassName)) {
                    final String rootprefix = rootDir.replace('/', '.');
                    final String pathsuffix = pathname.substring(rootprefix.length() + 1);
                    return setClassCache(stripDotClass(pathsuffix), stripDotClass(dotClassName));
                }
            }
        }

        // Now search the dirs
        for (final File subdir : subdirs) {
            final String nextdir = (prefix.length() == 0) ? subdir.getName() : prefix + "." + subdir.getName();
            final AnnotationMapping mapping = searchDirectory(subdir, rootDir, nextdir, dotClassName);
            if (mapping != null)
                return mapping;
        }

        return null;
    }

    public static AnnotationMapping getAnnotationMapping(final Object obj) throws HBqlException {
        return getAnnotationMapping(obj.getClass());
    }

    private static Map<Class<?>, AnnotationMapping> getAnnotationMappingMap() {
        return annotationMappingMap;
    }

    private static Map<String, Class<?>> getClassCacheMap() {
        return classCacheMap;
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

    public Object newInstance() throws IllegalAccessException, InstantiationException {
        return this.getClazz().newInstance();
    }

    public Object newObject(final List<SelectElement> selectElementList,
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
            throw new HBqlException("Cannot create new instance of " + this.getMappedSchema().getSchemaName());
        }

        return newobj;
    }

    private List<ColumnDescription> getColumnDescriptionList() {
        final List<ColumnDescription> varList = Lists.newArrayList();
        for (final ColumnAttrib columnAttrib : this.getColumnMap().values()) {
            final String columnType = columnAttrib.isAKeyAttrib()
                                      ? FieldType.KeyType.getFirstSynonym()
                                      : columnAttrib.getFieldType().getFirstSynonym();
            varList.add(ColumnDescription.newColumn(columnAttrib.getFamilyQualifiedName(),
                                                    columnAttrib.getAliasName(),
                                                    columnAttrib.isFamilyDefaultAttrib(),
                                                    columnType,
                                                    columnAttrib.isAnArray(),
                                                    null));
        }
        return varList;
    }

    private static String stripDotClass(final String str) {
        return str.substring(0, str.length() - ".class".length());
    }

    private static AnnotationMapping searchPackage(final String pkgName, final String objName) throws HBqlException {

        if (pkgName == null)
            return null;

        final String prefix = pkgName.replaceAll("/", ".");

        if (prefix.startsWith("META-INF.")
            || prefix.startsWith("antlr.")
            || prefix.startsWith("org.antlr.")
            || prefix.startsWith("apple.")
            || prefix.startsWith("com.apple.")
            || prefix.startsWith("sun.")
            || prefix.startsWith("com.sun.")
            || prefix.startsWith("java.")
            || prefix.startsWith("javax.")
            || prefix.startsWith("org.apache.zookeeper.")
            || prefix.startsWith("org.apache.bookkeeper.")
            || prefix.startsWith("org.apache.jute.")
            || prefix.startsWith("com.google.common.")
            || prefix.startsWith("org.apache.log4j.")
            || prefix.startsWith("junit.")
            || prefix.startsWith("org.junit.")
            || prefix.startsWith("org.xml.")
            || prefix.startsWith("org.w3c.")
            || prefix.startsWith("org.omg.")
            || prefix.startsWith("org.apache.mina.")
            || prefix.startsWith("org.apache.hadoop.")
            || prefix.startsWith("org.apache.commons.logging.")
            || prefix.startsWith("org.jcp.")
            || prefix.startsWith("org.slf4j.")
            || prefix.startsWith("org.ietf.")
            || prefix.startsWith("org.relaxng.")
            || prefix.startsWith("netscape.")
                )
            return null;

        final String fullname = prefix
                                + ((!prefix.endsWith(".") && prefix.length() > 0) ? "." : "")
                                + objName;

        return setClassCache(fullname, objName);
    }

    private static AnnotationMapping setClassCache(final String name, final String objName) throws HBqlException {

        final Class<?> clazz = getClass(name);
        if (clazz == null) {
            return null;
        }
        else {
            getClassCacheMap().put(objName, clazz);
            return getAnnotationMapping(clazz);
        }
    }

    private static Class getClass(final String str) throws HBqlException {
        try {
            final Class<?> clazz = Class.forName(str);

            // Make sure inner class is static
            if (clazz.isMemberClass() && !Modifier.isStatic(clazz.getModifiers())) {
                final String s = "Inner class " + clazz.getName() + " must be declared static";
                System.err.println(s);
                throw new HBqlException(s);
            }

            return clazz;
        }
        catch (ClassNotFoundException e) {
            return null;
        }
    }
}

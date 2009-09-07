package com.imap4j.hbase.hbql.schema;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.imap4j.hbase.antlr.config.HBqlRule;
import com.imap4j.hbase.hbql.EnvVars;
import com.imap4j.hbase.hbql.HColumn;
import com.imap4j.hbase.hbql.HColumnVersionMap;
import com.imap4j.hbase.hbql.HFamily;
import com.imap4j.hbase.hbql.HPersistException;
import com.imap4j.hbase.hbql.HPersistable;
import com.imap4j.hbase.hbql.HTable;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 19, 2009
 * Time: 5:30:59 PM
 */
public class ClassSchema implements Serializable {

    private final static Map<Class<?>, ClassSchema> classSchemaMap = Maps.newHashMap();
    private final static Map<String, Class<?>> classCacheMap = Maps.newHashMap();

    private final Map<String, VariableAttrib> variableAttribByVariableNameMap = Maps.newHashMap();

    private final Map<String, VersionAttrib> versionAttribByVariableNameMap = Maps.newHashMap();

    private final Map<String, List<ColumnAttrib>> fieldColumnListByFamilyNameMap = Maps.newHashMap();
    private final Map<String, ColumnAttrib> fieldColumnByFamilyQualifiedColumnNameMap = Maps.newHashMap();

    private final Class<?> clazz;
    private final HTable table;
    private final HFamily[] families;

    private ColumnAttrib keyColumnAttrib = null;

    public ClassSchema(final String desc) throws HPersistException {

        final List<VarDesc> varList = (List<VarDesc>)HBqlRule.SCHEMA.parse(desc);

        for (final VarDesc var : varList)
            setVariableAttribByVariableName(var.getVarName(), new VarDescAttrib(var.getVarName(), var.getType()));

        this.clazz = null;
        this.table = null;
        this.families = null;
    }

    public ClassSchema(final Class clazz) throws HPersistException {

        this.clazz = clazz;

        // Make sure there is an empty constructor declared
        try {
            this.getClazz().getConstructor();
        }
        catch (NoSuchMethodException e) {
            throw new HPersistException("Class " + this + " is missing a null constructor");
        }

        this.table = this.getClazz().getAnnotation(HTable.class);

        if (this.table == null)
            throw new HPersistException("Class " + this + " is missing @HTable annotation");

        this.families = this.table.families();

        if (this.families == null)
            throw new HPersistException("Class " + this + " is missing @HFamily values in @HTable annotation");

        for (final HFamily family : families) {
            final List<ColumnAttrib> attribs = Lists.newArrayList();
            this.fieldColumnListByFamilyNameMap.put(family.name(), attribs);
        }

        processAnnotations();
    }

    public Class<?> getClazz() {
        return this.clazz;
    }

    public Set<String> getFamilyNameList() {
        return this.fieldColumnListByFamilyNameMap.keySet();
    }

    public List<ColumnAttrib> getColumnAttribListByFamilyName(final String name) {
        return this.fieldColumnListByFamilyNameMap.get(name);
    }

    public ColumnAttrib getColumnAttribByFamilyQualifiedColumnName(final String name) {
        return this.fieldColumnByFamilyQualifiedColumnNameMap.get(name);
    }

    public void setColumnAttribByFamilyQualifiedColumnName(final String name, final ColumnAttrib columnAttrib) {
        this.fieldColumnByFamilyQualifiedColumnNameMap.put(name, columnAttrib);
    }

    public VariableAttrib getVariableAttribByVariableName(final String name) {
        return this.variableAttribByVariableNameMap.get(name);
    }

    public void setVariableAttribByVariableName(final String name, final VariableAttrib variableAttrib) {
        this.variableAttribByVariableNameMap.put(name, variableAttrib);
    }

    private static Map<Class<?>, ClassSchema> getClassSchemaMap() {
        return classSchemaMap;
    }

    public ColumnAttrib getKeyColumnAttrib() {
        return this.keyColumnAttrib;
    }

    public HFamily[] getFamilies() {
        return this.families;
    }

    public static ClassSchema getClassSchema(final HPersistable obj) throws HPersistException {
        final Class<?> clazz = obj.getClass();
        return getClassSchema(clazz);
    }

    public static ClassSchema getClassSchema(final String objname) throws HPersistException {

        // First see if already cached
        Class<?> clazz = classCacheMap.get(objname);

        if (clazz != null)
            return getClassSchema(clazz);

        // Then check with packagepath prefixes
        for (final String val : EnvVars.getPackagePath()) {
            String cp = val;
            if (!cp.endsWith(".") && cp.length() > 0)
                cp += ".";
            final String name = cp + objname;
            clazz = getClass(name);
            if (clazz != null) {
                classCacheMap.put(objname, clazz);
                return getClassSchema(clazz);
            }
        }

        throw new HPersistException("Cannot find " + objname + " in packagepath");
    }

    public List<String> getFieldList() {
        final List<String> retval = Lists.newArrayList();
        for (final VariableAttrib attrib : this.variableAttribByVariableNameMap.values()) {

            if (attrib.isKey())
                continue;

            retval.add(attrib.getVariableName());
        }
        return retval;
    }

    private static Class getClass(final String str) {
        try {
            return Class.forName(str);
        }
        catch (ClassNotFoundException e) {
            return null;
        }
    }

    public static ClassSchema getClassSchema(final Class<?> clazz) throws HPersistException {

        ClassSchema classSchema = classSchemaMap.get(clazz);
        if (classSchema != null)
            return classSchema;

        synchronized (getClassSchemaMap()) {
            // Check again in case waiting for the lock
            classSchema = getClassSchemaMap().get(clazz);
            if (classSchema != null)
                return classSchema;

            classSchema = new ClassSchema(clazz);
            getClassSchemaMap().put(clazz, classSchema);
            return classSchema;
        }
    }

    public String toString() {
        return this.getClazz().getName();
    }

    private void processAnnotations() throws HPersistException {

        // First process all HColumn fields so we can do lookup from HColumnVersionMaps
        for (final Field field : this.getClazz().getDeclaredFields())
            if (field.getAnnotation(HColumn.class) != null)
                this.processColumnAnnotation(field);

        if (this.getKeyColumnAttrib() == null)
            throw new HPersistException("Class " + this + " is missing an instance variable "
                                        + "annotated with @HColumn(key=true)");

        for (final Field field : this.getClazz().getDeclaredFields())
            if (field.getAnnotation(HColumnVersionMap.class) != null)
                this.processColumnVersionAnnotation(field);

    }

    private void processColumnAnnotation(final Field field) throws HPersistException {

        final ColumnAttrib attrib = new CurrentValueAttrib(field);

        if (attrib.isKey()) {
            if (this.getKeyColumnAttrib() != null)
                throw new HPersistException("Class " + this + " has multiple instance variables "
                                            + "annotated with @HColumn(key=true)");

            this.keyColumnAttrib = attrib;
        }
        else {
            final String family = attrib.getFamilyName();

            if (family.length() == 0)
                throw new HPersistException(attrib.getObjectQualifiedName()
                                            + " is missing family name in annotation");

            if (!this.fieldColumnListByFamilyNameMap.containsKey(family))
                throw new HPersistException(attrib.getObjectQualifiedName() + " references unknown family: " + family);

            this.getColumnAttribListByFamilyName(family).add(attrib);
        }

        this.setVariableAttribByVariableName(field.getName(), attrib);
        this.setColumnAttribByFamilyQualifiedColumnName(attrib.getFamilyQualifiedName(), attrib);
    }

    private void processColumnVersionAnnotation(final Field field) throws HPersistException {

        final VersionAttrib versionAttrib = VersionAttrib.createVersionAttrib(this, field);

        this.versionAttribByVariableNameMap.put(versionAttrib.getVariableName(), versionAttrib);

    }

    public String getTableName() {
        final String tableName = this.table.name();
        return (tableName.length() > 0) ? tableName : clazz.getSimpleName();
    }

    public boolean constainsVariableName(final String varname) {
        return this.variableAttribByVariableNameMap.containsKey(varname);
    }
}

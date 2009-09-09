package com.imap4j.hbase.hbql.schema;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
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
public class ExprSchema implements Serializable {

    private final static Map<Class<?>, ExprSchema> exprSchemaMap = Maps.newHashMap();
    private final static Map<String, Class<?>> classCacheMap = Maps.newHashMap();

    private final Map<String, VariableAttrib> variableAttribByVariableNameMap = Maps.newHashMap();

    private final Map<String, VersionAttrib> versionAttribByFamilyQualifiedColumnNameMap = Maps.newHashMap();

    private final Map<String, List<ColumnAttrib>> columnAtrtibListByFamilyNameMap = Maps.newHashMap();
    private final Map<String, ColumnAttrib> columnAttribByFamilyQualifiedColumnNameMap = Maps.newHashMap();

    private final Class<?> clazz;
    private final HTable table;
    private final HFamily[] families;

    private ColumnAttrib keyColumnAttrib = null;


    public ExprSchema(final List<VarDesc> varList) {

        for (final VarDesc var : varList) {
            final VarDescAttrib attrib = new VarDescAttrib(var.getVarName(), var.getType());
            setVariableAttribByVariableName(var.getVarName(), attrib);
        }

        this.clazz = null;
        this.table = null;
        this.families = null;
    }

    public ExprSchema(final Class clazz) throws HPersistException {

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
            this.columnAtrtibListByFamilyNameMap.put(family.name(), attribs);
        }

        processAnnotations();
    }

    public Class<?> getClazz() {
        return this.clazz;
    }

    public Set<String> getFamilyNameList() {
        return this.columnAtrtibListByFamilyNameMap.keySet();
    }

    public List<ColumnAttrib> getColumnAttribListByFamilyName(final String name) {
        return this.columnAtrtibListByFamilyNameMap.get(name);
    }

    public VersionAttrib getVersionAttribByFamilyQualifiedColumnName(final String name) {
        return this.versionAttribByFamilyQualifiedColumnNameMap.get(name);
    }

    public ColumnAttrib getColumnAttribByFamilyQualifiedColumnName(final String name) {
        return this.columnAttribByFamilyQualifiedColumnNameMap.get(name);
    }

    public void setColumnAttribByFamilyQualifiedColumnName(final String name, final ColumnAttrib columnAttrib) {
        this.columnAttribByFamilyQualifiedColumnNameMap.put(name, columnAttrib);
    }

    public VariableAttrib getVariableAttribByVariableName(final String name) {
        return this.variableAttribByVariableNameMap.get(name);
    }

    public void setVariableAttribByVariableName(final String name, final VariableAttrib variableAttrib) {
        this.variableAttribByVariableNameMap.put(name, variableAttrib);
    }

    private static Map<Class<?>, ExprSchema> getExprSchemaMap() {
        return exprSchemaMap;
    }

    public ColumnAttrib getKeyColumnAttrib() {
        return this.keyColumnAttrib;
    }

    public HFamily[] getFamilies() {
        return this.families;
    }

    public static ExprSchema getExprSchema(final HPersistable obj) throws HPersistException {
        final Class<?> clazz = obj.getClass();
        return getExprSchema(clazz);
    }

    public static ExprSchema getExprSchema(final String objname) throws HPersistException {

        // First see if already cached
        Class<?> clazz = classCacheMap.get(objname);

        if (clazz != null)
            return getExprSchema(clazz);

        // Then check with packagepath prefixes
        for (final String val : EnvVars.getPackagePath()) {
            String cp = val;
            if (!cp.endsWith(".") && cp.length() > 0)
                cp += ".";
            final String name = cp + objname;
            clazz = getClass(name);
            if (clazz != null) {
                classCacheMap.put(objname, clazz);
                return getExprSchema(clazz);
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

    public static ExprSchema getExprSchema(final Class<?> clazz) throws HPersistException {

        ExprSchema exprSchema = exprSchemaMap.get(clazz);
        if (exprSchema != null)
            return exprSchema;

        synchronized (getExprSchemaMap()) {
            // Check again in case waiting for the lock
            exprSchema = getExprSchemaMap().get(clazz);
            if (exprSchema != null)
                return exprSchema;

            exprSchema = new ExprSchema(clazz);
            getExprSchemaMap().put(clazz, exprSchema);
            return exprSchema;
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

        final ColumnAttrib columnAttrib = new CurrentValueAttrib(field);

        if (columnAttrib.isKey()) {
            if (this.getKeyColumnAttrib() != null)
                throw new HPersistException("Class " + this + " has multiple instance variables "
                                            + "annotated with @HColumn(key=true)");

            this.keyColumnAttrib = columnAttrib;
        }
        else {
            final String family = columnAttrib.getFamilyName();

            if (family.length() == 0)
                throw new HPersistException(columnAttrib.getObjectQualifiedName()
                                            + " is missing family name in annotation");

            if (!this.columnAtrtibListByFamilyNameMap.containsKey(family))
                throw new HPersistException(columnAttrib.getObjectQualifiedName() + " references unknown family: " + family);

            this.getColumnAttribListByFamilyName(family).add(columnAttrib);
        }

        this.setVariableAttribByVariableName(field.getName(), columnAttrib);
        this.setColumnAttribByFamilyQualifiedColumnName(columnAttrib.getFamilyQualifiedName(), columnAttrib);
    }

    private void processColumnVersionAnnotation(final Field field) throws HPersistException {
        final VersionAttrib versionAttrib = VersionAttrib.createVersionAttrib(this, field);

        this.versionAttribByFamilyQualifiedColumnNameMap.put(versionAttrib.getFamilyQualifiedName(), versionAttrib);

        this.setVariableAttribByVariableName(versionAttrib.getVariableName(), versionAttrib);
    }

    public String getTableName() {
        final String tableName = this.table.name();
        return (tableName.length() > 0) ? tableName : clazz.getSimpleName();
    }

    public boolean constainsVariableName(final String varname) {
        return this.variableAttribByVariableNameMap.containsKey(varname);
    }
}

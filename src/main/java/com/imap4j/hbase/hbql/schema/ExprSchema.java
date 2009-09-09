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
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.TokenStream;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.Collection;
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

    private final Map<String, List<ColumnAttrib>> columnAttribListByFamilyNameMap = Maps.newHashMap();
    private final Map<String, ColumnAttrib> columnAttribByFamilyQualifiedColumnNameMap = Maps.newHashMap();

    private final Class<?> clazz;
    private final HTable table;
    private final HFamily[] families;

    private ColumnAttrib keyColumnAttrib = null;


    public ExprSchema(final TokenStream input, final List<VarDesc> varList) throws RecognitionException {

        try {
            for (final VarDesc var : varList) {
                final VarDescAttrib attrib = new VarDescAttrib(var.getVarName(), var.getType());
                setVariableAttribByVariableName(var.getVarName(), attrib);
            }
        }
        catch (HPersistException e) {
            System.out.println(e.getMessage());
            throw new RecognitionException(input);
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
            this.setColumnAttribListByFamilyName(family.name(), attribs);
        }

        processAnnotations();
    }

    private static Map<Class<?>, ExprSchema> getExprSchemaMap() {
        return exprSchemaMap;
    }

    private static Map<String, Class<?>> getClassCacheMap() {
        return classCacheMap;
    }

    public Class<?> getClazz() {
        return this.clazz;
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
        Class<?> clazz = getClassCacheMap().get(objname);

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
                getClassCacheMap().put(objname, clazz);
                return getExprSchema(clazz);
            }
        }

        throw new HPersistException("Cannot find " + objname + " in packagepath");
    }

    public List<String> getFieldList() {
        final List<String> retval = Lists.newArrayList();
        for (final VariableAttrib attrib : this.getVariableAttribs()) {
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

        ExprSchema exprSchema = getExprSchemaMap().get(clazz);
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

            if (!this.containsFamilyName(family))
                throw new HPersistException(columnAttrib.getObjectQualifiedName()
                                            + " references unknown family: " + family);

            this.getColumnAttribListByFamilyName(family).add(columnAttrib);
        }

        this.setVariableAttribByVariableName(field.getName(), columnAttrib);
        this.setColumnAttribByFamilyQualifiedColumnName(columnAttrib.getFamilyQualifiedName(), columnAttrib);
    }

    private void processColumnVersionAnnotation(final Field field) throws HPersistException {
        final VersionAttrib versionAttrib = VersionAttrib.createVersionAttrib(this, field);
        this.setVersionAttribByFamilyQualifiedColumnName(versionAttrib.getFamilyQualifiedName(), versionAttrib);
        this.setVariableAttribByVariableName(versionAttrib.getVariableName(), versionAttrib);
    }

    public String getTableName() {
        final String tableName = this.table.name();
        return (tableName.length() > 0) ? tableName : clazz.getSimpleName();
    }

    // *** variableAttribByVariableNameMap calls
    private Map<String, VariableAttrib> getVariableAttribByVariableNameMap() {
        return this.variableAttribByVariableNameMap;
    }

    public boolean constainsVariableName(final String varname) {
        return this.getVariableAttribByVariableNameMap().containsKey(varname);
    }

    public Collection<VariableAttrib> getVariableAttribs() {
        return this.getVariableAttribByVariableNameMap().values();
    }

    public VariableAttrib getVariableAttribByVariableName(final String name) {
        return this.getVariableAttribByVariableNameMap().get(name);
    }

    private void setVariableAttribByVariableName(final String name,
                                                 final VariableAttrib variableAttrib) throws HPersistException {
        if (this.getVariableAttribByVariableNameMap().containsKey(name))
            throw new HPersistException(name + " already delcared");
        this.getVariableAttribByVariableNameMap().put(name, variableAttrib);
    }

    // *** columnAttribByFamilyQualifiedColumnNameMap calls
    private Map<String, ColumnAttrib> getColumnAttribByFamilyQualifiedColumnNameMap() {
        return this.columnAttribByFamilyQualifiedColumnNameMap;
    }

    public ColumnAttrib getColumnAttribByFamilyQualifiedColumnName(final String s) {
        return this.getColumnAttribByFamilyQualifiedColumnNameMap().get(s);
    }

    private void setColumnAttribByFamilyQualifiedColumnName(final String s,
                                                            final ColumnAttrib columnAttrib) throws HPersistException {
        if (this.getColumnAttribByFamilyQualifiedColumnNameMap().containsKey(s))
            throw new HPersistException(s + " already delcared");
        this.getColumnAttribByFamilyQualifiedColumnNameMap().put(s, columnAttrib);
    }

    // *** versionAttribByFamilyQualifiedColumnNameMap calls
    private Map<String, VersionAttrib> getVersionAttribByFamilyQualifiedColumnNameMap() {
        return versionAttribByFamilyQualifiedColumnNameMap;
    }

    public VersionAttrib getVersionAttribByFamilyQualifiedColumnName(final String s) {
        return this.getVersionAttribByFamilyQualifiedColumnNameMap().get(s);
    }

    private void setVersionAttribByFamilyQualifiedColumnName(final String s,
                                                             final VersionAttrib versionAttrib) throws HPersistException {
        if (this.getVersionAttribByFamilyQualifiedColumnNameMap().containsKey(s))
            throw new HPersistException(s + " already delcared");

        this.getVersionAttribByFamilyQualifiedColumnNameMap().put(s, versionAttrib);
    }

    // *** columnAttribListByFamilyNameMap
    private Map<String, List<ColumnAttrib>> getColumnAttribListByFamilyNameMap() {
        return columnAttribListByFamilyNameMap;
    }

    public Set<String> getFamilyNameList() {
        return this.getColumnAttribListByFamilyNameMap().keySet();
    }

    public List<ColumnAttrib> getColumnAttribListByFamilyName(final String s) {
        return this.getColumnAttribListByFamilyNameMap().get(s);
    }

    private boolean containsFamilyName(final String s) {
        return this.getColumnAttribListByFamilyNameMap().containsKey(s);
    }

    public void setColumnAttribListByFamilyName(final String s,
                                                final List<ColumnAttrib> columnAttribs) throws HPersistException {
        if (this.containsFamilyName(s))
            throw new HPersistException(s + " already delcared");
        this.getColumnAttribListByFamilyNameMap().put(s, columnAttribs);
    }
}

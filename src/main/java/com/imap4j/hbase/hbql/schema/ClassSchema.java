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
import java.lang.reflect.Modifier;
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

    private final Map<String, List<FieldAttrib>> fieldAttribListByFamilyNameMap = Maps.newHashMap();
    private final Map<String, FieldAttrib> fieldAttribByQualifiedColumnNameMap = Maps.newHashMap();
    private final Map<String, FieldAttrib> fieldAttribByVariableNameMap = Maps.newHashMap();
    private final Map<String, VersionAttrib> versionAttribByVariableNameMap = Maps.newHashMap();

    private final Class<?> clazz;
    private final HTable table;
    private final HFamily[] families;

    private FieldAttrib keyFieldAttrib = null;

    public ClassSchema(final String desc) throws HPersistException {

        final List<VarDesc> varList = (List<VarDesc>)HBqlRule.SCHEMA.parse(desc);

        for (final VarDesc var : varList)
            setFieldAttribByVariableName(var.getVarName(), new FieldAttrib(var.getVarName(), var.getType()));

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
            final List<FieldAttrib> attribs = Lists.newArrayList();
            this.fieldAttribListByFamilyNameMap.put(family.name(), attribs);
        }

        processFieldAnnotations();
    }

    public Class<?> getClazz() {
        return this.clazz;
    }

    public Set<String> getFamilyNameList() {
        return this.fieldAttribListByFamilyNameMap.keySet();
    }

    public List<FieldAttrib> getFieldAttribListByFamilyName(final String name) {
        return this.fieldAttribListByFamilyNameMap.get(name);
    }

    public FieldAttrib getFieldAttribByQualifiedColumnName(final String name) {
        return this.fieldAttribByQualifiedColumnNameMap.get(name);
    }

    public void setFieldAttribByQualifiedColumnName(final String name, final FieldAttrib fieldAttrib) {
        this.fieldAttribByQualifiedColumnNameMap.put(name, fieldAttrib);
    }

    public FieldAttrib getFieldAttribByVariableName(final String name) {
        return this.fieldAttribByVariableNameMap.get(name);
    }

    public void setFieldAttribByVariableName(final String name, final FieldAttrib fieldAttrib) {
        this.fieldAttribByVariableNameMap.put(name, fieldAttrib);
    }

    private static Map<Class<?>, ClassSchema> getClassSchemaMap() {
        return classSchemaMap;
    }

    public FieldAttrib getKeyFieldAttrib() {
        return this.keyFieldAttrib;
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
        for (final FieldAttrib attrib : this.fieldAttribByVariableNameMap.values()) {

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

    private void processFieldAnnotations() throws HPersistException {

        // First process all HColumn fields so we can do lookup from HColumnVersionMaps
        for (final Field field : this.getClazz().getDeclaredFields())
            if (field.getAnnotation(HColumn.class) != null)
                this.processColumnAnnotation(field);

        if (this.getKeyFieldAttrib() == null)
            throw new HPersistException("Class " + this + " is missing an instance variable "
                                        + "annotated with @HColumn(key=true)");

        for (final Field field : this.getClazz().getDeclaredFields())
            if (field.getAnnotation(HColumnVersionMap.class) != null)
                this.processColumnVersionMapAnnotation(field);

    }

    private void processColumnAnnotation(final Field field) throws HPersistException {

        if (isFinal(field))
            throw new HPersistException(this + "." + field.getName() + " cannot have a @HColumn "
                                        + "annotation and be marked final");

        final FieldAttrib attrib = new FieldAttrib(this.getClazz(), field, field.getAnnotation(HColumn.class));

        this.setFieldAttribByQualifiedColumnName(attrib.getQualifiedName(), attrib);
        this.setFieldAttribByVariableName(field.getName(), attrib);

        if (attrib.isKey()) {
            if (this.getKeyFieldAttrib() != null)
                throw new HPersistException("Class " + this + " has multiple instance variables "
                                            + "annotated with @HColumn(key=true)");

            this.keyFieldAttrib = attrib;
        }
        else {
            final String family = attrib.getFamilyName();
            if (!this.fieldAttribListByFamilyNameMap.containsKey(family))
                throw new HPersistException("Class " + this + " is missing @HFamily value for " + family);

            this.getFieldAttribListByFamilyName(family).add(attrib);
        }

    }

    private void processColumnVersionMapAnnotation(final Field field) throws HPersistException {

        HColumnVersionMap columnVersion = field.getAnnotation(HColumnVersionMap.class);
        VersionAttrib versionAttrib = new VersionAttrib(field, columnVersion);

        // Check if instance variable exists
        if (!this.fieldAttribByVariableNameMap.containsKey(columnVersion.instance())) {
            throw new HPersistException("The @HColumnVersionMap annotation for " + versionAttrib.getVariableName()
                                        + " refers to invalid instance variable " + columnVersion.instance());
        }

        // Check if it is a Map
        Class[] classes = field.getType().getInterfaces();

        this.versionAttribByVariableNameMap.put(versionAttrib.getVariableName(), versionAttrib);

    }

    private static boolean implementsInterface(final Object object, final Class clazz) {

    }

    private static boolean isFinal(final Field field) {

        final boolean isFinal = Modifier.isFinal(field.getModifiers());

        if (isFinal)
            return true;

        // Unlock private vars
        if (!field.isAccessible())
            field.setAccessible(true);

        return false;
    }

    public String getTableName() {
        final String tableName = this.table.name();
        return (tableName.length() > 0) ? tableName : clazz.getSimpleName();
    }

}

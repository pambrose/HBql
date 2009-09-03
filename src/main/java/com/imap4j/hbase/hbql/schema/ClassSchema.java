package com.imap4j.hbase.hbql.schema;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.imap4j.hbase.hbql.EnvVars;
import com.imap4j.hbase.hbql.HColumn;
import com.imap4j.hbase.hbql.HFamily;
import com.imap4j.hbase.hbql.HPersistException;
import com.imap4j.hbase.hbql.HPersistable;
import com.imap4j.hbase.hbql.HTable;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 19, 2009
 * Time: 5:30:59 PM
 */
public class ClassSchema implements Serializable {

    private final static Map<Class<?>, ClassSchema> classSchemaMap = Maps.newHashMap();
    private final static Map<String, Class<?>> classCacheMap = Maps.newHashMap();

    final Map<String, List<FieldAttrib>> fieldAttribMapByFamily = Maps.newHashMap();
    final Map<String, FieldAttrib> fieldAttribMapByField = Maps.newHashMap();
    final Map<String, FieldAttrib> fieldAttribMapByColumnName = Maps.newHashMap();
    final Map<String, FieldAttrib> fieldAttribMapByVarName = Maps.newHashMap();

    private final Class<?> clazz;
    private final HTable table;
    private final HFamily[] families;

    private FieldAttrib keyFieldAttrib = null;

    public ClassSchema(final String desc) throws HPersistException {
        // Format: varname:type, varname:type

    }

    public ClassSchema(final Class clazz) throws HPersistException {
        this.clazz = clazz;

        // Make sure there is a an empty constructor declared
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
            this.getFieldAttribMapByFamily().put(family.name(), attribs);
        }

        processFieldAnnotations();
    }

    public Class<?> getClazz() {
        return clazz;
    }

    public Map<String, List<FieldAttrib>> getFieldAttribMapByFamily() {
        return fieldAttribMapByFamily;
    }

    public Map<String, FieldAttrib> getFieldAttribMapByFieldAttrib() {
        return fieldAttribMapByField;
    }

    public Map<String, FieldAttrib> getFieldAttribMapByColumnName() {
        return fieldAttribMapByColumnName;
    }

    public Map<String, FieldAttrib> getFieldAttribMapByVarName() {
        return fieldAttribMapByVarName;
    }

    private static Map<Class<?>, ClassSchema> getClassSchemaMap() {
        return classSchemaMap;
    }

    public FieldAttrib getKeyFieldAttrib() {
        return keyFieldAttrib;
    }

    public HFamily[] getFamilies() {
        return this.families;
    }

    public FieldAttrib getFieldAttribByName(final String attribName) {
        return getFieldAttribMapByFieldAttrib().get(attribName);
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
        for (final FieldAttrib attrib : this.getFieldAttribMapByFieldAttrib().values()) {

            if (attrib.isKey())
                continue;

            retval.add(attrib.getFieldName());
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

        for (final Field field : this.getClazz().getDeclaredFields()) {

            final HColumn column = field.getAnnotation(HColumn.class);

            // Check if persisted or not
            if (column != null) {

                final boolean isFinal = checkFieldModifiers(field);

                if (isFinal)
                    throw new HPersistException(this + "." + field.getName() + " cannot have a @HColumn "
                                                + "annotation and be marked final");

                final FieldAttrib attrib = new FieldAttrib(this.getClazz(), field, column);

                this.getFieldAttribMapByFieldAttrib().put(field.getName(), attrib);
                this.getFieldAttribMapByColumnName().put(attrib.getQualifiedName(), attrib);
                this.getFieldAttribMapByVarName().put(field.getName(), attrib);

                if (attrib.isKey()) {
                    if (keyFieldAttrib != null)
                        throw new HPersistException("Class " + this + " has multiple instance variables "
                                                    + "annotated with @HColumn(key=true)");

                    keyFieldAttrib = attrib;
                }
                else {
                    final String family = attrib.getFamilyName();
                    if (!this.getFieldAttribMapByFamily().containsKey(family))
                        throw new HPersistException("Class " + this + " is missing @HFamily value for " + family);

                    this.getFieldAttribMapByFamily().get(family).add(attrib);
                }
            }
        }
        if (keyFieldAttrib == null)
            throw new HPersistException("Class " + this + " is missing an instance variable "
                                        + "annotated with @HColumn(key=true)");

    }

    private static boolean checkFieldModifiers(final Field field) {

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

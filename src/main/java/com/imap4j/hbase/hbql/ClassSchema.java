package com.imap4j.hbase.hbql;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

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
public class ClassSchema {

    private final static Map<Class<?>, ClassSchema> classSchemaMap = Maps.newHashMap();

    private final static Map<String, Class<?>> classCacheMap = Maps.newHashMap();

    private final Class<?> clazz;

    private final HBTable table;
    private final HBFamily[] families;

    private FieldAttrib keyFieldAttrib = null;

    final Map<String, List<FieldAttrib>> fieldAttribMapByFamily = Maps.newHashMap();
    final Map<String, FieldAttrib> fieldAttribMapByField = Maps.newHashMap();
    final Map<String, FieldAttrib> fieldAttribMapByColumn = Maps.newHashMap();

    public ClassSchema(final Class clazz) throws HBPersistException {
        this.clazz = clazz;

        // Make sure there is a an empty constructor declared
        try {
            this.getClazz().getConstructor();
        }
        catch (NoSuchMethodException e) {
            throw new HBPersistException("Class " + this + " is missing a null constructor");
        }

        this.table = this.getClazz().getAnnotation(HBTable.class);

        if (this.table == null)
            throw new HBPersistException("Class " + this + " is missing @HBTable annotation");

        this.families = this.table.families();

        if (this.families == null)
            throw new HBPersistException("Class " + this + " is missing @HBFamily values in @HBTable annotation");

        for (final HBFamily family : families) {
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

    public Map<String, FieldAttrib> getFieldAttribMapByField() {

        return fieldAttribMapByField;
    }

    public Map<String, FieldAttrib> getFieldAttribMapByColumn() {
        return fieldAttribMapByColumn;
    }

    private static Map<Class<?>, ClassSchema> getClassSchemaMap() {
        return classSchemaMap;
    }

    public FieldAttrib getKeyFieldAttrib() {
        return keyFieldAttrib;
    }

    public HBFamily[] getFamilies() {
        return this.families;
    }

    public FieldAttrib getFieldAttribByField(final String attribName) {
        return getFieldAttribMapByField().get(attribName);
    }

    public static ClassSchema getClassSchema(final HBPersistable obj) throws HBPersistException {
        final Class<?> clazz = obj.getClass();
        return getClassSchema(clazz);
    }

    public static ClassSchema getClassSchema(final String objname) throws HBPersistException {

        // First see if already cached
        Class<?> clazz = classCacheMap.get(objname);

        if (clazz != null)
            return getClassSchema(clazz);

        // Then check with classpath prefixes
        for (final String val : EnvVars.getClasspath()) {
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

        throw new HBPersistException("Cannot find " + objname + " in classpath");
    }

    public List<String> getFieldList() {
        final List<String> retval = Lists.newArrayList();
        for (final FieldAttrib attrib : this.getFieldAttribMapByField().values()) {

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

    public static ClassSchema getClassSchema(final Class<?> clazz) throws HBPersistException {

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

    private void processFieldAnnotations() throws HBPersistException {

        for (final Field field : this.getClazz().getDeclaredFields()) {

            final HBColumn column = field.getAnnotation(HBColumn.class);

            // Check if persisted or not
            if (column != null) {

                final boolean isFinal = checkFieldModifiers(field);

                if (isFinal)
                    throw new HBPersistException(this + "." + field.getName()
                                                 + " cannot have a @HBColumn annotation and be marked final");

                final FieldAttrib attrib = new FieldAttrib(this.getClazz(), field, column);

                this.getFieldAttribMapByField().put(field.getName(), attrib);
                this.getFieldAttribMapByColumn().put(attrib.getQualifiedName(), attrib);

                if (attrib.isKey()) {
                    if (keyFieldAttrib != null)
                        throw new HBPersistException("Class " + this + " has multiple instance variables "
                                                     + "annotated with @HBColumn(key=true)");

                    keyFieldAttrib = attrib;
                }
                else {
                    final String family = attrib.getFamilyName();
                    if (!this.getFieldAttribMapByFamily().containsKey(family))
                        throw new HBPersistException("Class " + this + " is missing @HBFamily value for " + family);

                    this.getFieldAttribMapByFamily().get(family).add(attrib);
                }
            }
        }
        if (keyFieldAttrib == null)
            throw new HBPersistException("Class " + this
                                         + " is missing an instance variable annotated with @HBColumn(key=true)");

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

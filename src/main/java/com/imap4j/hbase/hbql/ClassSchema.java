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

    private String tableName = null;

    private FieldAttrib keyFieldAttrib = null;

    final Map<String, List<FieldAttrib>> fieldAttribMapByFamily = Maps.newHashMap();
    final Map<String, FieldAttrib> fieldAttribMapByField = Maps.newHashMap();
    final Map<String, FieldAttrib> fieldAttribMapByColumn = Maps.newHashMap();

    public ClassSchema(final Class clazz) throws PersistException {
        this.clazz = clazz;

        // TODO check to make sure there is a an empty constructor declared

        processAnnotations();
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

    public static ClassSchema getClassSchema(final Persistable obj) throws PersistException {
        final Class<?> clazz = obj.getClass();
        return getClassSchema(clazz);
    }

    public static ClassSchema getClassSchema(final String objname) throws PersistException {

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

        throw new PersistException("Cannot find " + objname + " in classpath");
    }

    private static Class getClass(final String str) {
        try {
            return Class.forName(str);
        }
        catch (ClassNotFoundException e) {
            return null;
        }
    }

    public static ClassSchema getClassSchema(final Class<?> clazz) throws PersistException {

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

    private void processAnnotations() throws PersistException {

        final Table tabAnno = this.getClazz().getAnnotation(Table.class);

        if (tabAnno == null)
            throw new PersistException("Class " + this + " is missing @Table annotation");

        final String tabName = tabAnno.name();
        this.tableName = (tabName.length() > 0) ? tabName : clazz.getSimpleName();

        for (final Field field : this.getClazz().getDeclaredFields()) {

            final Column column = field.getAnnotation(Column.class);

            // Check if persisted or not
            if (column != null) {

                // TODO Deal with enums

                final boolean isFinal = checkFieldModifiers(field);

                if (isFinal)
                    throw new PersistException(this + "." + field.getName()
                                               + " cannot have a Column annotation and be marked final");

                final FieldAttrib attrib = new FieldAttrib(this.getClazz(), field, column);
                this.getFieldAttribMapByField().put(field.getName(), attrib);
                this.getFieldAttribMapByColumn().put(attrib.getQualifiedName(), attrib);

                if (column.key()) {
                    if (keyFieldAttrib != null)
                        throw new PersistException("Class " + this
                                                   + " has multiple instance variables annotated "
                                                   + "with Column(key=true)");

                    keyFieldAttrib = attrib;
                }
                else {
                    final String family = attrib.getFamilyName();
                    final List<FieldAttrib> columns;
                    if (!this.getFieldAttribMapByFamily().containsKey(family)) {
                        columns = Lists.newArrayList();
                        this.getFieldAttribMapByFamily().put(family, columns);
                    }
                    else {
                        columns = this.getFieldAttribMapByFamily().get(family);
                    }
                    columns.add(attrib);
                }
            }
        }
        if (keyFieldAttrib == null)
            throw new PersistException("Class " + this
                                       + " is missing an instance variable annotated with @Column(key=true)");

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
        return this.tableName;
    }

}

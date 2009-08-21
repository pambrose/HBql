package com.imap4j.hbase.hbql;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.lang.reflect.Field;
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

    private String tableName;

    final Map<String, List<FieldAttrib>> fieldAttribs = Maps.newHashMap();

    public ClassSchema(final Class clazz) throws PersistException {
        this.clazz = clazz;
        lookupAnnotations();
    }

    public Class<?> getClazz() {
        return clazz;
    }

    public Map<String, List<FieldAttrib>> getFieldAttribs() {
        return fieldAttribs;
    }

    private static Map<Class<?>, ClassSchema> getClassSchemaMap() {
        return classSchemaMap;
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

    private void lookupAnnotations() throws PersistException {

        final Table tabAnno = this.getClazz().getAnnotation(Table.class);

        if (tabAnno == null)
            throw new PersistException("Class " + this.getClazz().getName() + " is missing @Table");

        final String tabName = tabAnno.name();
        this.tableName = (tabName.length() > 0) ? tabName : clazz.getSimpleName();

        for (final Field field : this.getClazz().getDeclaredFields()) {

            final Column column = field.getAnnotation(Column.class);

            // Persisted or not
            if (column != null) {
                final FieldAttrib attrib = new FieldAttrib(this.getClazz(), field, column);
                final String family = attrib.getFamily();
                final List<FieldAttrib> columns;
                if (!this.getFieldAttribs().containsKey(family)) {
                    columns = Lists.newArrayList();
                    this.getFieldAttribs().put(family, columns);
                }
                else {
                    columns = this.getFieldAttribs().get(family);
                }
                columns.add(attrib);
            }
        }
    }

    public String getTableName() {
        return this.tableName;
    }

}

package com.imap4j.hbase.hbsql;

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

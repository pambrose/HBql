package com.imap4j.hbase;

import com.google.common.collect.Maps;

import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 19, 2009
 * Time: 5:30:59 PM
 */
public class ClassSchema {

    private final static Map<Class<?>, ClassSchema> fieldAttribMap = Maps.newHashMap();

    private final Class<?> clazz;

    private String tableName;

    final Map<String, String> columns = Maps.newHashMap();

    public <T extends Persistable> ClassSchema(final Class<T> clazz) throws PersistException {
        this.clazz = clazz;
        assignMeta();
    }

    public static <T extends Persistable> ClassSchema getMetaData(final Class<T> clazz) throws PersistException {

        ClassSchema classSchema = fieldAttribMap.get(clazz);
        if (classSchema != null)
            return classSchema;

        synchronized (fieldAttribMap) {

            classSchema = fieldAttribMap.get(clazz);
            if (classSchema != null)
                return classSchema;

            classSchema = new ClassSchema(clazz);
            fieldAttribMap.put(clazz, classSchema);
            return classSchema;
        }

    }

    private void assignMeta() throws PersistException {

        final Table tabAnno = this.getClazz().getAnnotation(Table.class);

        if (tabAnno == null)
            throw new PersistException("Class " + this.getClazz().getName() + " is missing @Table");

        final String tabName = tabAnno.name();
        this.tableName = (tabName != null && tabName.length() > 0) ? tabName : clazz.getSimpleName();

    }

    public Class<?> getClazz() {
        return clazz;
    }
}

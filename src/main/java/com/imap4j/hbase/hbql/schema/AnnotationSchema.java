package com.imap4j.hbase.hbql.schema;

import com.imap4j.hbase.hbase.HColumn;
import com.imap4j.hbase.hbase.HColumnVersionMap;
import com.imap4j.hbase.hbase.HFamily;
import com.imap4j.hbase.hbase.HPersistException;
import com.imap4j.hbase.hbase.HPersistable;
import com.imap4j.hbase.hbase.HTable;
import com.imap4j.hbase.util.Lists;
import com.imap4j.hbase.util.Maps;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 9, 2009
 * Time: 9:47:01 AM
 */
public class AnnotationSchema extends ExprSchema {

    private final static Map<Class<?>, AnnotationSchema> annotationSchemaMap = Maps.newHashMap();
    private final static Map<String, Class<?>> classCacheMap = Maps.newHashMap();

    private final Map<String, List<ColumnAttrib>> columnAttribListByFamilyNameMap = Maps.newHashMap();
    private final Map<String, ColumnAttrib> columnAttribByFamilyQualifiedColumnNameMap = Maps.newHashMap();
    private final Map<String, VersionAttrib> versionAttribByFamilyQualifiedColumnNameMap = Maps.newHashMap();

    private final Class<?> clazz;
    private final HTable table;
    private final HFamily[] families;

    private ColumnAttrib keyColumnAttrib = null;

    private AnnotationSchema(final Class clazz) throws HPersistException {

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

    public synchronized static AnnotationSchema getAnnotationSchema(final String objname) throws HPersistException {

        // First see if already cached
        Class<?> clazz = getClassCacheMap().get(objname);

        if (clazz != null)
            return getAnnotationSchema(clazz);

        // Then check with packagepath prefixes
        for (final String val : EnvVars.getPackagePath()) {
            String cp = val;
            if (!cp.endsWith(".") && cp.length() > 0)
                cp += ".";
            final String name = cp + objname;
            clazz = getClass(name);
            if (clazz != null) {
                getClassCacheMap().put(objname, clazz);
                return getAnnotationSchema(clazz);
            }
        }

        throw new HPersistException("Cannot find " + objname + " in packagepath");
    }

    public static AnnotationSchema getAnnotationSchema(final Object obj) throws HPersistException {
        return getAnnotationSchema(obj.getClass());
    }

    public synchronized static AnnotationSchema getAnnotationSchema(final Class<?> clazz) throws HPersistException {

        AnnotationSchema schema = getAnnotationSchemaMap().get(clazz);
        if (schema != null)
            return schema;

        schema = new AnnotationSchema(clazz);
        getAnnotationSchemaMap().put(clazz, schema);
        return schema;
    }

    private static Class getClass(final String str) {
        try {
            return Class.forName(str);
        }
        catch (ClassNotFoundException e) {
            return null;
        }
    }

    private static Map<Class<?>, AnnotationSchema> getAnnotationSchemaMap() {
        return annotationSchemaMap;
    }

    private static Map<String, Class<?>> getClassCacheMap() {
        return classCacheMap;
    }

    public String toString() {
        return this.getClazz().getName();
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

        this.addVariableAttrib(columnAttrib);
        this.setColumnAttribByFamilyQualifiedColumnName(columnAttrib.getFamilyQualifiedName(), columnAttrib);
    }

    private void processColumnVersionAnnotation(final Field field) throws HPersistException {
        final VersionAttrib versionAttrib = VersionAttrib.newVersionAttrib(this, field);
        this.setVersionAttribByFamilyQualifiedColumnName(versionAttrib.getFamilyQualifiedName(), versionAttrib);
        this.addVariableAttrib(versionAttrib);
    }

    public String getTableName() {
        final String tableName = this.table.name();
        return (tableName.length() > 0) ? tableName : clazz.getSimpleName();
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

}

package com.imap4j.hbase.hbql.schema;

import com.imap4j.hbase.hbase.HColumn;
import com.imap4j.hbase.hbase.HColumnVersionMap;
import com.imap4j.hbase.hbase.HFamily;
import com.imap4j.hbase.hbase.HPersistException;
import com.imap4j.hbase.hbase.HTable;
import com.imap4j.hbase.hbql.io.Serialization;
import com.imap4j.hbase.util.Lists;
import com.imap4j.hbase.util.Maps;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 9, 2009
 * Time: 9:47:01 AM
 */
public class AnnotationSchema extends HBaseSchema {

    private final static Map<Class<?>, AnnotationSchema> annotationSchemaMap = Maps.newHashMap();
    private final static Map<String, Class<?>> classCacheMap = Maps.newHashMap();

    private final Map<String, List<ColumnAttrib>> columnAttribListByFamilyNameMap = Maps.newHashMap();

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

        return null;
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

    @Override
    public String toString() {
        return this.getSchemaName();
    }

    @Override
    public String getSchemaName() {
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

    @Override
    public Object newInstance() throws IllegalAccessException, InstantiationException {
        return this.getClazz().newInstance();
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

    @Override
    public Object getObject(final Serialization ser,
                            final List<String> fieldList,
                            final int maxVersions,
                            final Result result) throws HPersistException {

        try {
            // Create object and assign key value
            final Object newobj = createNewObject(ser, result);

            // Assign most recent values
            assignCurrentValues(ser, fieldList, result, newobj);

            // Assign the versioned values
            if (maxVersions > 1)
                assignVersionedValues(ser, fieldList, result, newobj);

            return newobj;
        }
        catch (Exception e) {
            e.printStackTrace();
            throw new HPersistException("Error in getObject()");
        }
    }

    private Object createNewObject(final Serialization ser,
                                   final Result result) throws IOException, HPersistException {

        // Create new instance and set key value
        final ColumnAttrib keyattrib = this.getKeyColumnAttrib();
        final Object newobj;
        try {
            newobj = this.newInstance();
            final byte[] keybytes = result.getRow();
            keyattrib.setValue(ser, newobj, keybytes);
        }
        catch (InstantiationException e) {
            throw new RuntimeException("Cannot create new instance of " + this.getSchemaName());
        }
        catch (IllegalAccessException e) {
            throw new RuntimeException("Cannot set value for key  " + keyattrib.getVariableName()
                                       + " for " + this.getSchemaName());
        }
        return newobj;
    }

    private void assignCurrentValues(final Serialization ser,
                                     final List<String> fieldList,
                                     final Result result,
                                     final Object newobj) throws IOException, HPersistException {

        for (final KeyValue keyValue : result.list()) {

            final byte[] cbytes = keyValue.getColumn();
            final byte[] vbytes = result.getValue(cbytes);
            final String colname = ser.getStringFromBytes(cbytes);

            if (colname.endsWith("]")) {
                final int lbrace = colname.indexOf("[");
                final String mapcolumn = colname.substring(0, lbrace);
                final String mapKey = colname.substring(lbrace + 1, colname.length() - 1);
                final ColumnAttrib attrib = this.getColumnAttribByFamilyQualifiedColumnName(mapcolumn);
                final Object val = attrib.getValueFromBytes(ser, newobj, vbytes);

                Map mapval = (Map)attrib.getValue(newobj);

                // TODO Need to check if variable was on select list like below

                if (mapval == null) {
                    mapval = Maps.newHashMap();
                    attrib.setValue(newobj, mapval);
                }

                mapval.put(mapKey, val);
            }
            else {
                final ColumnAttrib attrib = this.getColumnAttribByFamilyQualifiedColumnName(colname);

                // Check if variable was requested in select list
                if (fieldList.contains(attrib.getVariableName()))
                    attrib.setValue(ser, newobj, vbytes);
            }
        }
    }

    private void assignVersionedValues(final Serialization ser,
                                       final List<String> fieldList,
                                       final Result result,
                                       final Object newobj) throws IOException, HPersistException {

        final NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> familyMap = result.getMap();

        for (final byte[] fbytes : familyMap.keySet()) {

            final String famname = ser.getStringFromBytes(fbytes) + ":";
            final NavigableMap<byte[], NavigableMap<Long, byte[]>> columnMap = familyMap.get(fbytes);

            for (final byte[] cbytes : columnMap.keySet()) {
                final String colname = ser.getStringFromBytes(cbytes);
                final String qualifiedName = famname + colname;
                final NavigableMap<Long, byte[]> tsMap = columnMap.get(cbytes);

                for (final Long timestamp : tsMap.keySet()) {
                    final byte[] vbytes = tsMap.get(timestamp);

                    final VersionAttrib attrib = this.getVersionAttribByFamilyQualifiedColumnName(qualifiedName);

                    // Ignore data if no version map exists for the column
                    if (attrib == null)
                        continue;

                    // Ignore if not in select list
                    if (!fieldList.contains(attrib.getField().getName()))
                        continue;

                    final Object val = attrib.getValueFromBytes(ser, newobj, vbytes);
                    Map mapval = (Map)attrib.getValue(newobj);

                    if (mapval == null) {
                        mapval = new TreeMap();
                        attrib.setValue(newobj, mapval);
                    }

                    mapval.put(timestamp, val);
                }
            }
        }
    }
}

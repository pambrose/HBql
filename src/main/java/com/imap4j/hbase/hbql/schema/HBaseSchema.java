package com.imap4j.hbase.hbql.schema;

import com.imap4j.hbase.hbase.HPersistException;
import com.imap4j.hbase.hbql.io.Serialization;
import com.imap4j.hbase.util.Maps;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 13, 2009
 * Time: 8:26:14 AM
 */
public abstract class HBaseSchema extends ExprSchema {

    private ColumnAttrib keyColumnAttrib = null;

    private final Map<String, ColumnAttrib> columnAttribByFamilyQualifiedColumnNameMap = Maps.newHashMap();
    private final Map<String, ColumnAttrib> versionAttribByFamilyQualifiedColumnNameMap = Maps.newHashMap();

    public Object newInstance() throws IllegalAccessException, InstantiationException {
        return null;
    }

    public ColumnAttrib getKeyColumnAttrib() {
        return this.keyColumnAttrib;
    }

    protected void setKeyColumnAttrib(final ColumnAttrib keyColumnAttrib) {
        this.keyColumnAttrib = keyColumnAttrib;
    }

    public abstract String getSchemaName();

    public abstract String getTableName();

    public abstract Object getObject(final Serialization ser,
                                     final List<String> fieldList,
                                     final int maxVersions,
                                     final Result result) throws HPersistException;


    public static HBaseSchema findSchema(final String tablename) throws HPersistException {

        // First look in defined schema, then try annotation schema
        HBaseSchema schema;

        schema = DefinedSchema.getDefinedSchema(tablename);
        if (schema != null)
            return schema;

        schema = AnnotationSchema.getAnnotationSchema(tablename);
        if (schema != null)
            return schema;

        throw new HPersistException("Unknown table: " + tablename);
    }

    // *** columnAttribByFamilyQualifiedColumnNameMap calls
    protected Map<String, ColumnAttrib> getColumnAttribByFamilyQualifiedColumnNameMap() {
        return this.columnAttribByFamilyQualifiedColumnNameMap;
    }

    public ColumnAttrib getColumnAttribByFamilyQualifiedColumnName(final String s) {
        return this.getColumnAttribByFamilyQualifiedColumnNameMap().get(s);
    }

    protected void addColumnAttrib(final ColumnAttrib attrib) throws HPersistException {
        final String name = attrib.getFamilyQualifiedName();
        if (this.getColumnAttribByFamilyQualifiedColumnNameMap().containsKey(name))
            throw new HPersistException(name + " already delcared");
        this.getColumnAttribByFamilyQualifiedColumnNameMap().put(name, attrib);
    }

    // *** versionAttribByFamilyQualifiedColumnNameMap calls
    private Map<String, ColumnAttrib> getVersionAttribByFamilyQualifiedColumnNameMap() {
        return versionAttribByFamilyQualifiedColumnNameMap;
    }

    public ColumnAttrib getVersionAttribByFamilyQualifiedColumnName(final String s) {
        return this.getVersionAttribByFamilyQualifiedColumnNameMap().get(s);
    }

    protected void addVersionAttrib(final ColumnAttrib attrib) throws HPersistException {
        final String name = attrib.getFamilyQualifiedName();
        if (this.getVersionAttribByFamilyQualifiedColumnNameMap().containsKey(name))
            throw new HPersistException(name + " already delcared");

        this.getVersionAttribByFamilyQualifiedColumnNameMap().put(name, attrib);
    }

    protected void assignCurrentValues(final Serialization ser,
                                       final List<String> fieldList,
                                       final Result result,
                                       final Object newobj) throws IOException, HPersistException {

        for (final KeyValue keyValue : result.list()) {

            final byte[] columnBytes = keyValue.getColumn();
            final String columnName = ser.getStringFromBytes(columnBytes);
            final long timestamp = keyValue.getTimestamp();
            final byte[] valueBytes = result.getValue(columnBytes);

            if (columnName.endsWith("]")) {
                final int lbrace = columnName.indexOf("[");
                final String mapcolumn = columnName.substring(0, lbrace);
                final String mapKey = columnName.substring(lbrace + 1, columnName.length() - 1);
                final ColumnAttrib attrib = this.getColumnAttribByFamilyQualifiedColumnName(mapcolumn);

                Map mapval = (Map)attrib.getCurrentValue(newobj);

                if (mapval == null) {
                    mapval = Maps.newHashMap();
                    // TODO Check this
                    attrib.setVersionedValueMap(newobj, mapval);
                }

                final Object val = attrib.getValueFromBytes(ser, newobj, valueBytes);
                mapval.put(mapKey, val);
            }
            else {
                final ColumnAttrib attrib = this.getColumnAttribByFamilyQualifiedColumnName(columnName);
                attrib.setCurrentValue(ser, newobj, timestamp, valueBytes);
            }
        }
    }

    protected void assignVersionedValues(final Serialization ser,
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

                    final ColumnAttrib attrib = this.getVersionAttribByFamilyQualifiedColumnName(qualifiedName);

                    // Ignore data if no version map exists for the column
                    if (attrib == null)
                        continue;

                    // Ignore if not in select list
                    if (!fieldList.contains(attrib.getVariableName()))
                        continue;

                    Map<Long, Object> mapval = (Map<Long, Object>)attrib.getVersionedValueMap(newobj);

                    if (mapval == null) {
                        mapval = new TreeMap();
                        attrib.setVersionedValueMap(newobj, mapval);
                    }

                    final Object val = attrib.getValueFromBytes(ser, newobj, vbytes);
                    mapval.put(timestamp, val);
                }
            }
        }
    }

    // This is relevant only for AnnotatedSchema
    public List<VarDesc> getVarDescList() {
        return null;
    }
}

package com.imap4j.hbase.hbql.schema;

import com.imap4j.hbase.hbase.HPersistException;
import com.imap4j.hbase.hbql.io.Serialization;
import com.imap4j.hbase.util.Maps;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 13, 2009
 * Time: 8:26:14 AM
 */
public abstract class HBaseSchema extends ExprSchema {

    private ColumnAttrib keyColumnAttrib = null;

    private final Map<String, ColumnAttrib> columnAttribByFamilyQualifiedColumnNameMap = Maps.newHashMap();
    private final Map<String, VersionAttrib> versionAttribByFamilyQualifiedColumnNameMap = Maps.newHashMap();

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

        // First look in annotation schema, thentry defined schema
        HBaseSchema schema = AnnotationSchema.getAnnotationSchema(tablename);
        if (schema != null)
            return schema;

        schema = DefinedSchema.getDefinedSchema(tablename);
        if (schema != null)
            return schema;

        throw new HPersistException("Unknown annotated table: " + tablename);
    }

    // *** columnAttribByFamilyQualifiedColumnNameMap calls
    private Map<String, ColumnAttrib> getColumnAttribByFamilyQualifiedColumnNameMap() {
        return this.columnAttribByFamilyQualifiedColumnNameMap;
    }

    public ColumnAttrib getColumnAttribByFamilyQualifiedColumnName(final String s) {
        return this.getColumnAttribByFamilyQualifiedColumnNameMap().get(s);
    }

    protected void setColumnAttribByFamilyQualifiedColumnName(final String s,
                                                              final ColumnAttrib columnAttrib) throws HPersistException {
        if (this.getColumnAttribByFamilyQualifiedColumnNameMap().containsKey(s))
            throw new HPersistException(s + " already delcared");
        this.getColumnAttribByFamilyQualifiedColumnNameMap().put(s, columnAttrib);
    }

    // *** versionAttribByFamilyQualifiedColumnNameMap calls
    private Map<String, VersionAttrib> getVersionAttribByFamilyQualifiedColumnNameMap() {
        return versionAttribByFamilyQualifiedColumnNameMap;
    }

    public VersionAttrib getVersionAttribByFamilyQualifiedColumnName(final String s) {
        return this.getVersionAttribByFamilyQualifiedColumnNameMap().get(s);
    }

    protected void setVersionAttribByFamilyQualifiedColumnName(final String s,
                                                               final VersionAttrib versionAttrib) throws HPersistException {
        if (this.getVersionAttribByFamilyQualifiedColumnNameMap().containsKey(s))
            throw new HPersistException(s + " already delcared");

        this.getVersionAttribByFamilyQualifiedColumnNameMap().put(s, versionAttrib);
    }

    protected void assignCurrentValues(final Serialization ser,
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

                Map mapval = (Map)attrib.getCurrentValue(newobj);

                if (mapval == null) {
                    mapval = Maps.newHashMap();
                    attrib.setVersionedValue(newobj, mapval);
                }

                mapval.put(mapKey, val);
            }
            else {
                final ColumnAttrib attrib = this.getColumnAttribByFamilyQualifiedColumnName(colname);
                attrib.setCurrentValue(ser, newobj, vbytes);
            }
        }
    }

}

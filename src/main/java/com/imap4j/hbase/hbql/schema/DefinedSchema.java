package com.imap4j.hbase.hbql.schema;

import com.imap4j.hbase.hbase.HPersistException;
import com.imap4j.hbase.hbase.HRecord;
import com.imap4j.hbase.hbql.io.Serialization;
import com.imap4j.hbase.util.Maps;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.TokenStream;
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
 * Date: Sep 9, 2009
 * Time: 10:19:00 AM
 */
public class DefinedSchema extends HBaseSchema {

    private final static Map<String, DefinedSchema> definedSchemaMap = Maps.newHashMap();

    final String tableName;

    public DefinedSchema(final TokenStream input, final List<VarDesc> varList) throws RecognitionException {
        this.tableName = "declared";
        try {
            for (final VarDesc var : varList) {
                final VarDescAttrib attrib = new VarDescAttrib(var);
                addVariableAttrib(attrib);
                this.setColumnAttribByFamilyQualifiedColumnName(var.getVariableName(), attrib);
            }
        }
        catch (HPersistException e) {
            System.out.println(e.getMessage());
            throw new RecognitionException(input);
        }
    }

    private DefinedSchema(final String tableName, final List<VarDesc> varList) throws HPersistException {
        this.tableName = tableName;
        for (final VarDesc var : varList) {
            final VarDescAttrib attrib = new VarDescAttrib(var);
            addVariableAttrib(attrib);
            this.setColumnAttribByFamilyQualifiedColumnName(var.getVariableName(), attrib);
        }
    }

    public synchronized static DefinedSchema newDefinedSchema(final String tableName,
                                                              final List<VarDesc> varList) throws HPersistException {

        DefinedSchema schema = getDefinedSchemaMap().get(tableName);
        if (schema != null)
            throw new HPersistException("Table " + tableName + " already defined");

        schema = new DefinedSchema(tableName, varList);
        getDefinedSchemaMap().put(tableName, schema);
        return schema;
    }

    private static Map<String, DefinedSchema> getDefinedSchemaMap() {
        return definedSchemaMap;
    }

    public static DefinedSchema getDefinedSchema(final String tableName) {
        return getDefinedSchemaMap().get(tableName);
    }

    @Override
    public String getTableName() {
        return this.tableName;
    }

    @Override
    public String getSchemaName() {
        return this.getTableName();
    }

    @Override
    public HRecord getObject(final Serialization ser,
                             final List<String> fieldList,
                             final int maxVersions,
                             final Result result) throws HPersistException {

        try {
            // Create object and assign key value
            final HRecord newobj = createNewHRecord(ser, result);

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

    private HRecord createNewHRecord(final Serialization ser,
                                     final Result result) throws IOException, HPersistException {

        // Create new instance and set key value
        final HRecord newobj = new HRecord();
        final ColumnAttrib keyattrib = this.getKeyColumnAttrib();
        if (keyattrib != null) {
            final byte[] keybytes = result.getRow();
            keyattrib.setValue(ser, newobj, keybytes);
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
                // TODO Need to finish this
                /*
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
                */
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

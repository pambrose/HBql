package com.imap4j.hbase.hbql.schema;

import com.imap4j.hbase.hbase.HPersistException;
import com.imap4j.hbase.hbase.HRecord;
import com.imap4j.hbase.hbql.io.Serialization;
import com.imap4j.hbase.util.Maps;
import org.apache.hadoop.hbase.client.Result;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 9, 2009
 * Time: 10:19:00 AM
 */
public class DefinedSchema extends HBaseSchema {

    private final static Map<String, DefinedSchema> definedSchemaMap = Maps.newHashMap();

    final String tableName;

    public DefinedSchema(final List<VarDesc> varList) throws HPersistException {

        this.tableName = "declared";

        for (final VarDesc var : varList)
            processColumn(var);
    }

    private DefinedSchema(final String tableName, final List<VarDesc> varList) throws HPersistException {
        this.tableName = tableName;
        for (final VarDesc var : varList)
            processColumn(var);
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

    public synchronized static DefinedSchema newDefinedSchema(final HBaseSchema schema) throws HPersistException {
        return new DefinedSchema(schema.getTableName(), schema.getVarDescList());
    }

    private void processColumn(final VarDesc var) throws HPersistException {

        final VarDescAttrib attrib = new VarDescAttrib(var);

        this.addVariableAttrib(attrib);
        this.addColumnAttrib(attrib);
        this.addVersionAttrib(attrib);

        if (attrib.isKey()) {
            if (this.getKeyColumnAttrib() != null)
                throw new HPersistException("Table " + this + " has multiple instance variables "
                                            + "marked as keys");
            this.setKeyColumnAttrib(attrib);
        }
        else {
            final String family = attrib.getFamilyName();
            if (family.length() == 0)
                throw new HPersistException(attrib.getColumnName() + " is missing family name");

        }
    }

    private static Map<String, DefinedSchema> getDefinedSchemaMap() {
        return definedSchemaMap;
    }

    public static DefinedSchema getDefinedSchema(final String tableName) {
        return getDefinedSchemaMap().get(tableName);
    }

    @Override
    public String toString() {
        return this.getTableName();
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
        final HRecord newobj = new HRecord(this);
        final ColumnAttrib keyattrib = this.getKeyColumnAttrib();
        if (keyattrib != null) {
            final byte[] keybytes = result.getRow();
            keyattrib.setCurrentValue(ser, newobj, 0, keybytes);
        }
        return newobj;
    }
}

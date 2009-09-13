package com.imap4j.hbase.hbql.schema;

import com.imap4j.hbase.hbase.HPersistException;
import com.imap4j.hbase.hbase.HRecord;
import com.imap4j.hbase.hbql.io.Serialization;
import com.imap4j.hbase.util.Maps;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.TokenStream;
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
public class DeclaredSchema extends HBaseSchema {

    private final static Map<String, DeclaredSchema> declaredSchemaMap = Maps.newHashMap();

    final String tableName;

    public DeclaredSchema(final TokenStream input, final List<VarDesc> varList) throws RecognitionException {
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

    private DeclaredSchema(final String tableName, final List<VarDesc> varList) throws HPersistException {
        this.tableName = tableName;
        for (final VarDesc var : varList) {
            final VarDescAttrib attrib = new VarDescAttrib(var);
            addVariableAttrib(attrib);
            this.setColumnAttribByFamilyQualifiedColumnName(var.getVariableName(), attrib);
        }
    }

    public synchronized static DeclaredSchema newDeclaredSchema(final String tableName,
                                                                final List<VarDesc> varList) throws HPersistException {

        DeclaredSchema schema = getDeclaredSchemaMap().get(tableName);
        if (schema != null)
            throw new HPersistException("Table " + tableName + " already defined");

        schema = new DeclaredSchema(tableName, varList);
        getDeclaredSchemaMap().put(tableName, schema);
        return schema;
    }

    private static Map<String, DeclaredSchema> getDeclaredSchemaMap() {
        return declaredSchemaMap;
    }

    public static HBaseSchema getDeclaredSchema(final String tableName) {
        return getDeclaredSchemaMap().get(tableName);
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
            //  assignCurrentValues(ser, schema, fieldList, result, newobj);

            // Assign the versioned values
            //  if (maxVersions > 1)
            //      assignVersionedValues(schema, fieldList, result, newobj);

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

}

package org.apache.hadoop.hbase.hbql.query.schema;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.client.HRecord;
import org.apache.hadoop.hbase.hbql.query.io.Serialization;
import org.apache.hadoop.hbase.hbql.query.util.Lists;
import org.apache.hadoop.hbase.hbql.query.util.Maps;

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
    final String aliasName;

    public DefinedSchema(final List<VarDesc> varList) throws HPersistException {
        this.tableName = "embedded";
        this.aliasName = "embedded";
        for (final VarDesc var : varList)
            processColumn(var, false);
    }

    private DefinedSchema(final String tableName,
                          final String aliasName,
                          final List<VarDesc> varList) throws HPersistException {
        this.tableName = tableName;
        this.aliasName = aliasName;
        for (final VarDesc var : varList)
            processColumn(var, true);
    }

    public synchronized static DefinedSchema newDefinedSchema(final String tableName,
                                                              final String aliasName,
                                                              final List<VarDesc> varList) throws HPersistException {

        if (doesDefinedSchemaExist(tableName))
            throw new HPersistException("Table " + tableName + " already defined");

        if (aliasName != null && doesDefinedSchemaExist(aliasName))
            throw new HPersistException("Alias " + aliasName + " already defined");

        final DefinedSchema schema = new DefinedSchema(tableName, aliasName, varList);

        getDefinedSchemaMap().put(tableName, schema);

        // Add in the same schema if there is an alias
        if (aliasName != null && !tableName.equals(aliasName))
            getDefinedSchemaMap().put(aliasName, schema);

        return schema;
    }

    private static boolean doesDefinedSchemaExist(final String tableName) {
        final DefinedSchema schema = getDefinedSchemaMap().get(tableName);
        return schema != null;
    }

    public synchronized static DefinedSchema newDefinedSchema(final HBaseSchema schema) throws HPersistException {
        return new DefinedSchema(schema.getTableName(), null, schema.getVarDescList());
    }

    private void processColumn(final VarDesc var, final boolean enforceFamilyName) throws HPersistException {

        final VarDescAttrib attrib = new VarDescAttrib(var);

        this.addVariableAttribToVariableNameMap(attrib);
        this.addColumnAttribToFamilyQualifiedNameMap(attrib);
        this.addVersionAttribToFamilyQualifiedNameMap(attrib);
        this.addColumnAttribListToFamilyNameMap(attrib);

        if (attrib.isKeyAttrib()) {
            if (this.getKeyAttrib() != null)
                throw new HPersistException("Table " + this + " has multiple instance variables marked as keys");
            this.setKeyAttrib(attrib);
        }
        else {
            final String family = attrib.getFamilyName();
            if (enforceFamilyName && family.length() == 0)
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

    public String getAliasName() {
        return this.aliasName;
    }

    @Override
    public String getTableName() {
        return this.tableName;
    }

    @Override
    public List<HColumnDescriptor> getColumnDescriptors() {
        final List<HColumnDescriptor> descList = Lists.newArrayList();
        for (final String familyName : this.getFamilySet())
            descList.add(new HColumnDescriptor(familyName));

        return descList;
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
        final HRecord newrec = new HRecord();
        newrec.setSchema(this);
        final ColumnAttrib keyattrib = this.getKeyAttrib();
        if (keyattrib != null) {
            final byte[] keybytes = result.getRow();
            keyattrib.setCurrentValue(ser, newrec, 0, keybytes);
        }
        return newrec;
    }
}

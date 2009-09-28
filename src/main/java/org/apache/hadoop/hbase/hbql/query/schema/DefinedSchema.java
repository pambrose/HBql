package org.apache.hadoop.hbase.hbql.query.schema;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.HBqlFilter;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HRecord;
import org.apache.hadoop.hbase.hbql.query.antlr.HBql;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
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

    public DefinedSchema(final List<VarDesc> varList) throws HBqlException {
        this.tableName = "embedded";
        this.aliasName = "embedded";
        for (final VarDesc var : varList)
            processColumn(var, false);
    }

    private DefinedSchema(final String tableName,
                          final String aliasName,
                          final List<VarDesc> varList) throws HBqlException {
        this.tableName = tableName;
        this.aliasName = aliasName;
        for (final VarDesc var : varList)
            processColumn(var, true);
    }

    public synchronized static DefinedSchema newDefinedSchema(final String tableName,
                                                              final String aliasName,
                                                              final List<VarDesc> varList) throws HBqlException {

        if (doesDefinedSchemaExist(tableName))
            throw new HBqlException("Table " + tableName + " already defined");

        if (aliasName != null && doesDefinedSchemaExist(aliasName))
            throw new HBqlException("Alias " + aliasName + " already defined");

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

    public synchronized static DefinedSchema newDefinedSchema(final HBaseSchema schema) throws HBqlException {
        return new DefinedSchema(schema.getTableName(), null, schema.getVarDescList());
    }

    private void processColumn(final VarDesc var, final boolean enforceFamilyName) throws HBqlException {

        final DefinedAttrib attrib = new DefinedAttrib(var);

        this.addVariableAttribToVariableNameMap(attrib);
        this.addColumnAttribToFamilyQualifiedNameMap(attrib);
        this.addVersionAttribToFamilyQualifiedNameMap(attrib);
        this.addColumnAttribListToFamilyNameMap(attrib);

        if (attrib.isKeyAttrib()) {
            if (this.getKeyAttrib() != null)
                throw new HBqlException("Table " + this + " has multiple instance variables marked as keys");
            this.setKeyAttrib(attrib);
        }
        else {
            final String family = attrib.getFamilyName();
            if (enforceFamilyName && family.length() == 0)
                throw new HBqlException(attrib.getColumnName() + " is missing family name");

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
    public HRecord getObject(final List<String> fieldList,
                             final int maxVersions,
                             final Result result) throws HBqlException {

        try {
            // Create object and assign key value
            final HRecord newobj = createNewHRecord(result);

            // Assign most recent values
            assignCurrentValues(fieldList, result, newobj);

            // Assign the versioned values
            if (maxVersions > 1)
                assignVersionedValues(fieldList, result, newobj);

            return newobj;
        }
        catch (Exception e) {
            e.printStackTrace();
            throw new HBqlException("Error in getObject()");
        }

    }

    private HRecord createNewHRecord(final Result result) throws IOException, HBqlException {

        // Create new instance and set key value
        final HRecord newrec = new HRecord();
        newrec.setSchema(this);
        final ColumnAttrib keyattrib = this.getKeyAttrib();
        if (keyattrib != null) {
            final byte[] keybytes = result.getRow();
            keyattrib.setCurrentValue(newrec, 0, keybytes);
        }
        return newrec;
    }

    public Scan getScanForFields(final String... fields) throws IOException, HBqlException {

        final Scan scan = new Scan();

        for (final String field : fields) {
            final DefinedAttrib attrib = (DefinedAttrib)this.getVariableAttribByVariableName(field);
            if (attrib.isKeyAttrib())
                continue;
            scan.addColumn(attrib.getFamilyNameAsBytes(), attrib.getColumnNameAsBytes());
        }

        return scan;
    }

    public HBqlFilter newHBqlFilter(final String query) throws HBqlException {
        final ExprTree exprTree = HBql.parseWhereExpression(query, this);
        return new HBqlFilter(exprTree, -1);
    }
}

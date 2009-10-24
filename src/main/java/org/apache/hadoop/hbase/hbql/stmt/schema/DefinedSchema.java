package org.apache.hadoop.hbase.hbql.stmt.schema;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.HBqlFilter;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HRecord;
import org.apache.hadoop.hbase.hbql.query.impl.hbase.HRecordImpl;
import org.apache.hadoop.hbase.hbql.stmt.antlr.HBql;
import org.apache.hadoop.hbase.hbql.stmt.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.stmt.select.SelectElement;
import org.apache.hadoop.hbase.hbql.stmt.util.Lists;

import java.util.List;

public class DefinedSchema extends HBaseSchema {

    final String tableName;

    public DefinedSchema(final List<ColumnDescription> columnDescriptionList) throws HBqlException {
        super("embedded");
        this.tableName = "embedded";
        for (final ColumnDescription columnDescription : columnDescriptionList)
            this.processColumn(columnDescription, false);
    }

    public DefinedSchema(final String schemaName,
                         final String tableName,
                         final List<ColumnDescription> columnDescriptionList) throws HBqlException {
        super(schemaName);
        this.tableName = tableName;
        for (final ColumnDescription columnDescription : columnDescriptionList)
            processColumn(columnDescription, true);
    }

    private void processColumn(final ColumnDescription columnDescription,
                               final boolean requireFamilyName) throws HBqlException {

        final DefinedAttrib attrib = new DefinedAttrib(columnDescription);

        this.addAttribToVariableNameMap(attrib, attrib.getNamesForColumn());
        this.addAttribToFamilyQualifiedNameMap(attrib);
        this.addVersionAttrib(attrib);
        this.addFamilyDefaultAttrib(attrib);

        this.addAttribToFamilyNameColumnListMap(attrib);

        if (attrib.isKeyAttrib()) {
            if (this.getKeyAttrib() != null)
                throw new HBqlException("Schema " + this + " has multiple instance variables marked as keys");
            this.setKeyAttrib(attrib);
        }
        else {
            final String family = attrib.getFamilyName();
            if (requireFamilyName && family.length() == 0)
                throw new HBqlException(attrib.getColumnName() + " is missing family name");
        }
    }

    public HRecord newObject(final List<ColumnAttrib> selectAttribList,
                             final List<SelectElement> selectElementList,
                             final int maxVersions,
                             final Result result) throws HBqlException {

        // Create object and assign values
        final HRecordImpl newrec = new HRecordImpl(this);

        // Assign values
        this.assignSelectValues(newrec, selectAttribList, selectElementList, maxVersions, result);

        return newrec;
    }

    public List<HColumnDescriptor> getColumnDescriptors() {
        final List<HColumnDescriptor> descList = Lists.newArrayList();
        for (final String familyName : this.getFamilySet())
            descList.add(new HColumnDescriptor(familyName));
        return descList;
    }

    public HBqlFilter newHBqlFilter(final String query) throws HBqlException {
        final ExprTree exprTree = HBql.parseWhereExpression(query, this);
        return new HBqlFilter(exprTree, -1);
    }

    public String getTableName() {
        return this.tableName;
    }

    protected DefinedSchema getDefinedSchemaEquivalent() {
        return this;
    }
}

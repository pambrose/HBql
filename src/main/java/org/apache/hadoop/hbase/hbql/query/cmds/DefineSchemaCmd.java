package org.apache.hadoop.hbase.hbql.query.cmds;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HOutput;
import org.apache.hadoop.hbase.hbql.client.SchemaManager;
import org.apache.hadoop.hbase.hbql.query.schema.ColumnAttrib;
import org.apache.hadoop.hbase.hbql.query.schema.ColumnDescription;
import org.apache.hadoop.hbase.hbql.query.schema.DefinedSchema;

import java.util.List;

public class DefineSchemaCmd extends SchemaCmd implements SchemaManagerCmd {

    private final String tableName;
    private final List<ColumnDescription> columnDescriptionList;

    public DefineSchemaCmd(final String schemaName, final String tableName, final List<ColumnDescription> columnDescriptionList) {
        super(schemaName);
        this.tableName = (tableName == null || tableName.length() == 0) ? schemaName : tableName;
        this.columnDescriptionList = columnDescriptionList;
    }

    private String getTableName() {
        return tableName;
    }

    private List<ColumnDescription> getColumnDescriptionList() {
        return columnDescriptionList;
    }

    public HOutput execute() throws HBqlException {

        final DefinedSchema schema = SchemaManager.newDefinedSchema(this.getSchemaName(),
                                                                    this.getTableName(),
                                                                    this.getColumnDescriptionList());

        for (final ColumnAttrib attrib : schema.getColumnAttribSet()) {
            if (attrib.getFieldType() == null && !attrib.isFamilyDefaultAttrib())
                throw new HBqlException(schema.getSchemaName() + " attribute "
                                        + attrib.getFamilyQualifiedName() + " has unknown type.");
        }

        final HOutput retval = new HOutput();
        retval.out.println("Schema " + schema.getSchemaName() + " defined.");
        retval.out.flush();

        return retval;
    }
}
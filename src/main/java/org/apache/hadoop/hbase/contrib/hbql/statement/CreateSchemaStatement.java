package org.apache.hadoop.hbase.contrib.hbql.statement;

import org.apache.expreval.client.HBqlException;
import org.apache.hadoop.hbase.contrib.hbql.client.Output;
import org.apache.hadoop.hbase.contrib.hbql.client.SchemaManager;
import org.apache.hadoop.hbase.contrib.hbql.schema.ColumnAttrib;
import org.apache.hadoop.hbase.contrib.hbql.schema.ColumnDescription;
import org.apache.hadoop.hbase.contrib.hbql.schema.DefinedSchema;

import java.util.List;

public class CreateSchemaStatement extends SchemaStatement implements SchemaManagerStatement {

    private final String tableName;
    private final List<ColumnDescription> columnDescriptionList;

    public CreateSchemaStatement(final String schemaName,
                                 final String tableName,
                                 final List<ColumnDescription> columnDescriptionList) {
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

    public Output execute() throws HBqlException {

        final DefinedSchema schema = SchemaManager.newDefinedSchema(this.getSchemaName(),
                                                                    this.getTableName(),
                                                                    this.getColumnDescriptionList());

        for (final ColumnAttrib attrib : schema.getColumnAttribSet()) {
            if (attrib.getFieldType() == null && !attrib.isFamilyDefaultAttrib())
                throw new HBqlException(schema.getSchemaName() + " attribute "
                                        + attrib.getFamilyQualifiedName() + " has unknown type.");
        }

        return new Output("Schema " + schema.getSchemaName() + " defined.");
    }
}
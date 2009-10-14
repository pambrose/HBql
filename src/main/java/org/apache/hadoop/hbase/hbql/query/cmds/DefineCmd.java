package org.apache.hadoop.hbase.hbql.query.cmds;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HOutput;
import org.apache.hadoop.hbase.hbql.client.SchemaManager;
import org.apache.hadoop.hbase.hbql.query.schema.ColumnAttrib;
import org.apache.hadoop.hbase.hbql.query.schema.ColumnDescription;
import org.apache.hadoop.hbase.hbql.query.schema.DefinedSchema;

import java.util.List;

public class DefineCmd extends TableCmd implements SchemaManagerCmd {

    private String alias;
    private final List<ColumnDescription> columnDescriptionList;

    public DefineCmd(final String tableName, final String alias, final List<ColumnDescription> columnDescriptionList) {
        super(tableName);
        this.alias = alias;
        this.columnDescriptionList = columnDescriptionList;
    }

    private String getAlias() {
        return alias;
    }

    private List<ColumnDescription> getColumnDescriptionList() {
        return columnDescriptionList;
    }

    public HOutput execute() throws HBqlException {

        final DefinedSchema schema = SchemaManager.newDefinedSchema(this.getTableName(),
                                                                    this.getAlias(),
                                                                    this.getColumnDescriptionList());

        for (final ColumnAttrib attrib : schema.getColumnAttribSet()) {
            if (attrib.getFieldType() == null && !attrib.isFamilyDefaultAttrib())
                throw new HBqlException(schema.getTableName() + " attribute "
                                        + attrib.getFamilyQualifiedName() + " has unknown type.");
        }

        final HOutput retval = new HOutput();
        retval.out.println("Table " + schema.getTableName() + " defined.");
        retval.out.flush();

        return retval;
    }
}
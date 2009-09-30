package org.apache.hadoop.hbase.hbql.query.antlr.cmds;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HOutput;
import org.apache.hadoop.hbase.hbql.query.schema.ColumnDescription;
import org.apache.hadoop.hbase.hbql.query.schema.DefinedSchema;
import org.apache.hadoop.hbase.hbql.query.schema.VariableAttrib;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 24, 2009
 * Time: 10:31:14 PM
 */
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

    @Override
    public HOutput execute() throws HBqlException {

        final DefinedSchema schema = DefinedSchema.newDefinedSchema(this.getTableName(),
                                                                    this.getAlias(),
                                                                    this.getColumnDescriptionList());

        for (final VariableAttrib attrib : schema.getVariableAttribSet()) {
            if (attrib.getFieldType() == null)
                throw new HBqlException(schema.getTableName() + " attribute " + attrib.getFamilyQualifiedName()
                                        + " has unknown type.");
        }

        final HOutput retval = new HOutput();
        retval.out.println("Table " + schema.getTableName() + " defined.");
        retval.out.flush();
        return retval;
    }

}
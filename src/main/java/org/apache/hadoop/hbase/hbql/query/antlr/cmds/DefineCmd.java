package org.apache.hadoop.hbase.hbql.query.antlr.cmds;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HOutput;
import org.apache.hadoop.hbase.hbql.query.schema.ColumnDescription;
import org.apache.hadoop.hbase.hbql.query.schema.DefinedAttrib;
import org.apache.hadoop.hbase.hbql.query.schema.DefinedSchema;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 24, 2009
 * Time: 10:31:14 PM
 */
public class DefineCmd extends TableCmd implements SchemaManagerCmd {

    private String alias;
    private final List<ColumnDescription> varList;

    public DefineCmd(final String tableName, final String alias, final List<ColumnDescription> varList) {
        super(tableName);
        this.alias = alias;
        this.varList = varList;
    }

    private String getAlias() {
        return alias;
    }

    private List<ColumnDescription> getVarList() {
        return varList;
    }

    @Override
    public HOutput execute() throws HBqlException {

        final DefinedSchema schema = DefinedSchema.newDefinedSchema(this.getTableName(),
                                                                    this.getAlias(),
                                                                    this.getVarList());

        for (final String name : schema.getVariableAttribNames()) {

            final DefinedAttrib attrib = (DefinedAttrib)schema.getVariableAttribByVariableName(name);

            if (attrib.getFieldType() == null)
                throw new HBqlException(schema.getTableName() + " attribute " + attrib.getVariableName()
                                        + " has unknown type " + attrib.getTypeName());
        }

        final HOutput retval = new HOutput();
        retval.out.println("Table " + schema.getTableName() + " defined.");
        retval.out.flush();
        return retval;
    }

}
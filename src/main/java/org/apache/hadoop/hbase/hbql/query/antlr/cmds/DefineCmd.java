package org.apache.hadoop.hbase.hbql.query.antlr.cmds;

import org.apache.hadoop.hbase.hbql.client.HConnection;
import org.apache.hadoop.hbase.hbql.client.HOutput;
import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.schema.DefinedSchema;
import org.apache.hadoop.hbase.hbql.query.schema.VarDesc;
import org.apache.hadoop.hbase.hbql.query.schema.VarDescAttrib;
import org.apache.hadoop.hbase.hbql.query.schema.VariableAttrib;

import java.io.IOException;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 24, 2009
 * Time: 10:31:14 PM
 */
public class DefineCmd extends TableCmd {

    private String alias;
    private final List<VarDesc> varList;

    public DefineCmd(final String tableName, final String alias, final List<VarDesc> varList) {
        super(tableName);
        this.alias = alias;
        this.varList = varList;
    }

    private String getAlias() {
        return alias;
    }

    private List<VarDesc> getVarList() {
        return varList;
    }

    @Override
    public HOutput exec(final HConnection conn) throws HPersistException, IOException {

        final DefinedSchema schema = DefinedSchema.newDefinedSchema(this.getTableName(),
                                                                    this.getAlias(),
                                                                    this.getVarList());

        for (final VariableAttrib attrib : schema.getVariableAttribs()) {
            final VarDescAttrib vdattrib = (VarDescAttrib)attrib;
            if (attrib.getFieldType() == null)
                throw new HPersistException(schema.getTableName() + " attribute " + vdattrib.getVariableName()
                                            + " has unknown type " + vdattrib.getTypeName());
        }

        final HOutput retval = new HOutput();
        retval.out.println("Table " + schema.getTableName() + " defined.");
        retval.out.flush();
        return retval;
    }

}
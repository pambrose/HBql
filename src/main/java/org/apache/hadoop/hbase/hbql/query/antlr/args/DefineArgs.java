package org.apache.hadoop.hbase.hbql.query.antlr.args;

import org.apache.hadoop.hbase.hbql.query.schema.VarDesc;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 24, 2009
 * Time: 10:31:14 PM
 */
public class DefineArgs implements ExecArgs {

    private final String tableName;
    private final List<VarDesc> varList;

    public DefineArgs(final String tableName, final List<VarDesc> varList) {
        this.tableName = tableName;
        this.varList = varList;
    }

    public String getTableName() {
        return this.tableName;
    }

    public List<VarDesc> getVarList() {
        return varList;
    }
}
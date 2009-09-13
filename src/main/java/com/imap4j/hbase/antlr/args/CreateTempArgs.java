package com.imap4j.hbase.antlr.args;

import com.imap4j.hbase.hbql.schema.VarDesc;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 24, 2009
 * Time: 10:31:14 PM
 */
public class CreateTempArgs implements ExecArgs {

    private final String tableName;
    private final List<VarDesc> varList;

    public CreateTempArgs(final String tableName, final List<VarDesc> varList) {
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
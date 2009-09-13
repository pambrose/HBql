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

    private final String classname;
    private final List<VarDesc> varList;

    public CreateTempArgs(final String classname, final List<VarDesc> varList) {
        this.classname = classname;
        this.varList = varList;
    }

    public String getClassname() {
        return this.classname;
    }

    public List<VarDesc> getVarList() {
        return varList;
    }
}
package com.imap4j.hbase.hbql.schema;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 3, 2009
 * Time: 11:38:01 AM
 */
public class VarDesc {
    private String varname;
    private FieldType type;

    public VarDesc(final String varname, final String typename) {
        this.varname = varname;
        this.type = type;
    }
}



package com.imap4j.hbase.hbql.schema;

import com.imap4j.hbase.hbql.HPersistException;

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
        try {
            this.type = FieldType.getFieldType(typename);
        }
        catch (HPersistException e) {
            this.type = null;
        }
    }

    public String getVarname() {
        return this.varname;
    }

    public FieldType getType() {
        return this.type;
    }
}



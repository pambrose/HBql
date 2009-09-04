package com.imap4j.hbase.hbql.schema;

import com.google.common.collect.Lists;
import com.imap4j.hbase.hbql.HPersistException;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 3, 2009
 * Time: 11:38:01 AM
 */
public class VarDesc {
    private String varname;
    private FieldType type;

    private VarDesc(final String varname, final FieldType type) {
        this.varname = varname;
        this.type = type;
    }

    public static List<VarDesc> getList(final List<String> varList, final String typename) {

        final List<VarDesc> retval = Lists.newArrayList();
        FieldType vartype;
        try {
            vartype = FieldType.getFieldType(typename);
        }
        catch (HPersistException e) {
            vartype = null;
        }

        for (final String var : varList)
            retval.add(new VarDesc(var, vartype));

        return retval;
    }

    public String getVarname() {
        return this.varname;
    }

    public FieldType getType() {
        return this.type;
    }
}



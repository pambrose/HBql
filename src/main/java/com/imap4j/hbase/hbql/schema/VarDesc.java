package com.imap4j.hbase.hbql.schema;

import com.imap4j.hbase.hbase.HPersistException;
import com.imap4j.hbase.util.Lists;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 3, 2009
 * Time: 11:38:01 AM
 */
public class VarDesc {
    private String varName;
    private String typeName;
    private FieldType fieldType;

    private VarDesc(final String varName, final String typeName) {
        this.varName = varName;
        this.typeName = typeName;
        this.fieldType = getFieldType(this.getTypeName());
    }

    public static VarDesc newVarDesc(final String varName, final String typeName) {
        return new VarDesc(varName, typeName);
    }

    public static List<VarDesc> getList(final List<String> varList, final String typeName) {

        final List<VarDesc> retval = Lists.newArrayList();

        for (final String var : varList)
            retval.add(new VarDesc(var, typeName));

        return retval;
    }

    private static FieldType getFieldType(final String typeName) {
        try {
            return FieldType.getFieldType(typeName);
        }
        catch (HPersistException e) {
            return null;
        }

    }

    public String getVarName() {
        return this.varName;
    }

    public String getTypeName() {
        return this.typeName;
    }

    public FieldType getType() {
        return this.fieldType;
    }
}



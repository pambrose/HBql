package com.imap4j.hbase.hbql.schema;

import com.imap4j.hbase.hbase.HPersistException;
import com.imap4j.hbase.util.Lists;

import java.io.Serializable;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 3, 2009
 * Time: 11:38:01 AM
 */
public class VarDesc implements Serializable {
    private String variableName;
    private String qualifiedName;
    private String typeName;
    private FieldType fieldType;

    private VarDesc(final String variableName, final String qualifiedName, final String typeName) {
        this.variableName = (variableName == null) ? qualifiedName : variableName;
        this.qualifiedName = qualifiedName;
        this.typeName = typeName;
        this.fieldType = getFieldType(this.getTypeName());
    }

    public static VarDesc newVarDesc(final String variableName, final String qualifiedName, final String typeName) {
        return new VarDesc(variableName, qualifiedName, typeName);
    }

    public static List<VarDesc> getList(final List<String> varList, final String typeName) {

        final List<VarDesc> retval = Lists.newArrayList();

        for (final String var : varList)
            retval.add(new VarDesc(var, var, typeName));

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

    public String getFamilyName() {
        if (this.getQualifiedName().indexOf(":") != -1) {
            final String[] vals = this.getQualifiedName().split(":");
            return vals[0];
        }
        return "";
    }

    public String getColumnName() {
        if (this.getQualifiedName().indexOf(":") != -1) {
            final String[] vals = this.getQualifiedName().split(":");
            return vals[1];
        }
        return this.getVariableName();
    }

    public String getQualifiedName() {
        return qualifiedName;
    }

    public String getVariableName() {
        return this.variableName;
    }

    public String getTypeName() {
        return this.typeName;
    }

    public FieldType getFieldType() {
        return this.fieldType;
    }
}



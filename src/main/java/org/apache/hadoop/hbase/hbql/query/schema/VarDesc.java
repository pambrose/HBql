package org.apache.hadoop.hbase.hbql.query.schema;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.util.Lists;

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
    private String familyQualifiedName;
    private String typeName;
    private FieldType fieldType;

    private VarDesc(final String variableName, final String familyQualifiedName, final String typeName) {
        this.variableName = (variableName == null) ? familyQualifiedName : variableName;
        this.familyQualifiedName = familyQualifiedName;
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
        if (this.getFamilyQualifiedName().indexOf(":") != -1) {
            final String[] vals = this.getFamilyQualifiedName().split(":");
            return vals[0];
        }
        return "";
    }

    public String getColumnName() {
        if (this.getFamilyQualifiedName().indexOf(":") != -1) {
            final String[] vals = this.getFamilyQualifiedName().split(":");
            return vals[1];
        }
        return this.getVariableName();
    }

    public String getFamilyQualifiedName() {
        return this.familyQualifiedName;
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



package org.apache.hadoop.hbase.hbql.query.schema;

import org.apache.hadoop.hbase.hbql.client.HBqlException;

import java.io.Serializable;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 3, 2009
 * Time: 11:38:01 AM
 */
public class ColumnDescription implements Serializable {

    private String aliasName;
    private String familyQualifiedName;
    private String typeName;
    private FieldType fieldType;

    private ColumnDescription(final String aliasName, final String familyQualifiedName, final String typeName) {
        this.aliasName = (aliasName == null) ? familyQualifiedName : aliasName;
        this.familyQualifiedName = familyQualifiedName;
        this.typeName = typeName;
        this.fieldType = getFieldType(this.getTypeName());
    }

    public static ColumnDescription newColumnDescription(final String aliasName,
                                                         final String qualifiedName,
                                                         final String typeName) {
        return new ColumnDescription(aliasName, qualifiedName, typeName);
    }

    private static FieldType getFieldType(final String typeName) {
        try {
            return FieldType.getFieldType(typeName);
        }
        catch (HBqlException e) {
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
        return this.getAliasName();
    }

    public String getFamilyQualifiedName() {
        return this.familyQualifiedName;
    }

    public String getAliasName() {
        return this.aliasName;
    }

    public String getTypeName() {
        return this.typeName;
    }

    public FieldType getFieldType() {
        return this.fieldType;
    }
}



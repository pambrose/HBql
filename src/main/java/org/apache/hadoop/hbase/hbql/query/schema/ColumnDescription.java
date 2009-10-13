package org.apache.hadoop.hbase.hbql.query.schema;

import org.apache.hadoop.hbase.hbql.client.HBqlException;

import java.io.Serializable;

public class ColumnDescription implements Serializable {

    private final String aliasName;
    private final String familyName, columnName;
    private final boolean mapKeysAsColumns;
    private final boolean familyDefault;
    private final boolean isArray;
    private final FieldType fieldType;

    private ColumnDescription(final String familyQualifiedName,
                              final String aliasName,
                              final boolean mapKeysAsColumns,
                              final boolean familyDefault,
                              final String typeName,
                              final boolean isArray) {

        if (familyQualifiedName.indexOf(":") != -1) {
            final String[] names = familyQualifiedName.split(":");
            familyName = names[0];
            columnName = names[1];
        }
        else {
            familyName = "";
            columnName = familyQualifiedName;
        }

        this.aliasName = aliasName;
        this.mapKeysAsColumns = mapKeysAsColumns;
        this.familyDefault = familyDefault;
        this.fieldType = getFieldType(typeName);
        this.isArray = isArray;
    }

    public static ColumnDescription newColumn(final String familyQualifiedName,
                                              final String aliasName,
                                              final boolean mapKeysAsColumns,
                                              final boolean familyDefault,
                                              final String typeName,
                                              final boolean isArray) {
        return new ColumnDescription(familyQualifiedName,
                                     aliasName,
                                     mapKeysAsColumns,
                                     familyDefault,
                                     typeName,
                                     isArray);
    }

    public static ColumnDescription newFamilyDefault(final String familyQualifiedName, final String aliasName) {
        return new ColumnDescription(familyQualifiedName, aliasName, false, false, null, false);
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
        return this.familyName;
    }

    public String getColumnName() {
        return this.columnName;
    }

    public String getAliasName() {
        return this.aliasName;
    }

    public boolean isMapKeysAsColumns() {
        return this.mapKeysAsColumns;
    }

    public boolean isFamilyDefault() {
        return this.familyDefault;
    }

    public FieldType getFieldType() {
        return this.fieldType;
    }

    public boolean isArray() {
        return this.isArray;
    }
}



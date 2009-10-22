package org.apache.hadoop.hbase.hbql.query.cmds;

public abstract class SchemaCmd {

    private final String schemaName;

    protected SchemaCmd(final String schemaName) {
        this.schemaName = schemaName;
    }

    protected String getSchemaName() {
        return schemaName;
    }
}
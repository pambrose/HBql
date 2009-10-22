package org.apache.hadoop.hbase.hbql.query.cmds.schema;

public abstract class SchemaCommand {

    private final String schemaName;

    protected SchemaCommand(final String schemaName) {
        this.schemaName = schemaName;
    }

    protected String getSchemaName() {
        return schemaName;
    }
}
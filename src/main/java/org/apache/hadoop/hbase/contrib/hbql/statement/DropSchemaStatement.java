package org.apache.hadoop.hbase.contrib.hbql.statement;

import org.apache.expreval.client.HBqlException;
import org.apache.hadoop.hbase.contrib.hbql.client.Output;
import org.apache.hadoop.hbase.contrib.hbql.client.SchemaManager;

public class DropSchemaStatement extends SchemaStatement implements SchemaManagerStatement {

    public DropSchemaStatement(final String schemaName) {
        super(schemaName);
    }

    public Output execute() throws HBqlException {
        SchemaManager.dropSchema(this.getSchemaName());
        return new Output("Schema " + this.getSchemaName() + " dropped.");
    }
}
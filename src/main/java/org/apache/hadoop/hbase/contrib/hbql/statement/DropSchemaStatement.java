package org.apache.hadoop.hbase.contrib.hbql.statement;

import org.apache.expreval.client.HBqlException;
import org.apache.hadoop.hbase.contrib.hbql.client.HOutput;
import org.apache.hadoop.hbase.contrib.hbql.client.HSchemaManager;

public class DropSchemaStatement extends SchemaStatement implements SchemaManagerStatement {

    public DropSchemaStatement(final String schemaName) {
        super(schemaName);
    }

    public HOutput execute() throws HBqlException {
        HSchemaManager.dropSchema(this.getSchemaName());
        return new HOutput("Schema " + this.getSchemaName() + " dropped.");
    }
}
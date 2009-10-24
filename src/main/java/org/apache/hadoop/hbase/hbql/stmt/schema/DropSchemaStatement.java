package org.apache.hadoop.hbase.hbql.stmt.schema;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HOutput;
import org.apache.hadoop.hbase.hbql.stmt.SchemaManagerStatement;
import org.apache.hadoop.hbase.hbql.stmt.SchemaStatement;

public class DropSchemaStatement extends SchemaStatement implements SchemaManagerStatement {

    public DropSchemaStatement(final String schemaName) {
        super(schemaName);
    }

    public HOutput execute() throws HBqlException {

        org.apache.hadoop.hbase.hbql.client.SchemaManager.dropSchema(this.getSchemaName());

        final HOutput retval = new HOutput();
        retval.out.println("Schema " + this.getSchemaName() + " dropped.");
        retval.out.flush();

        return retval;
    }
}
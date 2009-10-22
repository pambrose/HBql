package org.apache.hadoop.hbase.hbql.query.cmds.schema;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HOutput;
import org.apache.hadoop.hbase.hbql.query.cmds.SchemaManagerCommand;

public class DropSchema extends SchemaCommand implements SchemaManagerCommand {

    public DropSchema(final String schemaName) {
        super(schemaName);
    }

    public HOutput execute() throws HBqlException {

        org.apache.hadoop.hbase.hbql.client.SchemaManager.dropSchema(this.getSchemaName());

        final HOutput retval = new HOutput();
        retval.out.println("Schmema " + this.getSchemaName() + " dropped.");
        retval.out.flush();

        return retval;
    }
}
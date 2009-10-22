package org.apache.hadoop.hbase.hbql.query.cmds;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HOutput;
import org.apache.hadoop.hbase.hbql.client.SchemaManager;

public class DropSchemaCmd extends SchemaCmd implements SchemaManagerCmd {

    public DropSchemaCmd(final String schemaName) {
        super(schemaName);
    }

    public HOutput execute() throws HBqlException {

        SchemaManager.dropSchema(this.getSchemaName());

        final HOutput retval = new HOutput();
        retval.out.println("Schmema " + this.getSchemaName() + " dropped.");
        retval.out.flush();

        return retval;
    }
}
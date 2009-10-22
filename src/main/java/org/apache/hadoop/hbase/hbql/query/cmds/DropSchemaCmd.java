package org.apache.hadoop.hbase.hbql.query.cmds;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HOutput;
import org.apache.hadoop.hbase.hbql.client.SchemaManager;

public class DropSchemaCmd extends TableCmd implements SchemaManagerCmd {

    public DropSchemaCmd(final String tableName) {
        super(tableName);
    }

    public HOutput execute() throws HBqlException {

        SchemaManager.dropSchema(this.getTableName());

        final HOutput retval = new HOutput();
        retval.out.println("Schmema " + this.getTableName() + " dropped.");
        retval.out.flush();

        return retval;
    }
}
package org.apache.hadoop.hbase.hbql.query.cmds;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HConnection;
import org.apache.hadoop.hbase.hbql.client.HOutput;
import org.apache.hadoop.hbase.hbql.query.schema.HBaseSchema;

import java.io.IOException;

public class DescribeTableCmd extends TableCmd implements ConnectionCmd {

    public DescribeTableCmd(final String tableName) {
        super(tableName);
    }

    public HOutput execute(final HConnection conn) throws HBqlException, IOException {

        final HBaseSchema schema = HBaseSchema.findSchema(this.getTableName());

        final HBaseAdmin admin = new HBaseAdmin(conn.getConfig());
        final HTableDescriptor tableDesc = admin.getTableDescriptor(schema.getTableNameAsBytes());

        final HOutput retval = new HOutput();
        retval.out.println("Table name: " + tableDesc.getNameAsString());
        retval.out.println("Families:");
        for (final HColumnDescriptor columnDesc : tableDesc.getFamilies()) {
            retval.out.println("\t" + columnDesc.getNameAsString()
                               + " Max Verions: " + columnDesc.getMaxVersions()
                               + " TTL: " + columnDesc.getTimeToLive()
                               + " Block Size: " + columnDesc.getBlocksize()
                               + " Compression: " + columnDesc.getCompression().getName()
                               + " Compression Type: " + columnDesc.getCompressionType().getName());
        }

        retval.out.flush();
        return retval;
    }
}
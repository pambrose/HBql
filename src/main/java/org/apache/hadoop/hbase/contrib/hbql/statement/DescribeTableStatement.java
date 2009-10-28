package org.apache.hadoop.hbase.contrib.hbql.statement;

import org.apache.expreval.client.HBqlException;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.contrib.hbql.client.HOutput;
import org.apache.hadoop.hbase.contrib.hbql.impl.ConnectionImpl;

import java.io.IOException;

public class DescribeTableStatement extends TableStatement implements ConnectionStatement {

    public DescribeTableStatement(final String tableName) {
        super(tableName);
    }

    public HOutput execute(final ConnectionImpl conn) throws HBqlException, IOException {

        final HTableDescriptor tableDesc = conn.getAdmin().getTableDescriptor(this.getTableName().getBytes());

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
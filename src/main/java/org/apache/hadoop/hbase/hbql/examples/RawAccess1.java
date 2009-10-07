package org.apache.hadoop.hbase.hbql.examples;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class RawAccess1 {

    public static void main(String[] args) throws IOException, HBqlException {

        final byte[] family = Bytes.toBytes("family1");
        final byte[] author = Bytes.toBytes("author");
        final byte[] title = Bytes.toBytes("title");

        HTable table = new HTable(new HBaseConfiguration(), "testobjects");

        for (int i = 0; i < 0; i++) {
            Put put = new Put(Bytes.toBytes("00000000" + i));
            put.add(family, author, Bytes.toBytes("A value for author"));
            table.put(put);
            table.flushCommits();
        }

        Scan scan = new Scan();
        scan.addColumn(family, author);
        ResultScanner scanner = table.getScanner(scan);

        for (Result result : scanner) {
            System.out.println(Bytes.toString(result.getRow()) + " - "
                               + Bytes.toString(result.getValue(family, author)) + " - "
                               + Bytes.toString(result.getValue(family, title)));
        }
    }
}

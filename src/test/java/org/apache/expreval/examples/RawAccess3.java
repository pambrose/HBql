package org.apache.expreval.examples;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.contrib.hbql.client.HBqlException;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class RawAccess3 {

    public static void main(String[] args) throws IOException, HBqlException {

        final byte[] family = Bytes.toBytes("f1");
        final byte[] col1 = Bytes.toBytes("val1");
        final byte[] col2 = Bytes.toBytes("val2");
        final byte[] col3 = Bytes.toBytes("val3");

        final HTable table = new HTable(new HBaseConfiguration(), "table1");

        final Scan scan = new Scan();
        scan.addColumn(family, col1);
        scan.addColumn(family, col2);
        scan.addColumn(family, col3);
        ResultScanner scanner = table.getScanner(scan);

        for (final Result result : scanner) {
            System.out.println(Bytes.toString(result.getRow()) + " - "
                               + Bytes.toInt(result.getValue(family, col1)) + " - "
                               + Bytes.toInt(result.getValue(family, col2)) + " - "
                               + Bytes.toInt(result.getValue(family, col3)));
        }
    }
}
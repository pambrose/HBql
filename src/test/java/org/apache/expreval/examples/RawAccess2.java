package org.apache.expreval.examples;

import org.apache.expreval.client.HBqlException;
import org.apache.expreval.util.Util;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class RawAccess2 {

    public static void main(String[] args) throws IOException, HBqlException {

        final byte[] family = Bytes.toBytes("f1");
        final byte[] col1 = Bytes.toBytes("val1");
        final byte[] col2 = Bytes.toBytes("val2");

        final HTable table = new HTable(new HBaseConfiguration(), "table1");

        for (int i = 40; i < 45; i++) {
            final Put put = new Put(Bytes.toBytes(Util.getZeroPaddedNumber(i, 10)));
            put.add(family, col1, Bytes.toBytes(341));
            put.add(family, col2, Bytes.toBytes(682));
            table.put(put);
            table.flushCommits();
        }

        final Scan scan = new Scan();
        scan.addColumn(family, col1);
        scan.addColumn(family, col2);
        ResultScanner scanner = table.getScanner(scan);

        for (final Result result : scanner) {
            System.out.println(Bytes.toString(result.getRow()) + " - "
                               + Bytes.toInt(result.getValue(family, col1)) + " - "
                               + Bytes.toInt(result.getValue(family, col2)));
        }
    }
}
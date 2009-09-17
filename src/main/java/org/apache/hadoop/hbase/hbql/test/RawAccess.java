package org.apache.hadoop.hbase.hbql.test;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.hbql.client.HPersistException;

import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 17, 2009
 * Time: 2:43:58 PM
 */
public class RawAccess {

    public static void main(String[] args) throws IOException, HPersistException {

        HTable table = new HTable(new HBaseConfiguration(), "testobjects");

        for (int i = 0; i < 5; i++) {
            Put put = new Put(("00000000" + i).getBytes());
            put.add("family1".getBytes(), "author".getBytes(), "A value for author".getBytes());
            table.put(put);
            table.flushCommits();
        }

        Scan scan = new Scan();
        scan.addColumn("family1".getBytes(), "author".getBytes());
        ResultScanner scanner = table.getScanner(scan);

        for (Result result : scanner) {
            String val = new String(result.getRow());
        }

    }
}

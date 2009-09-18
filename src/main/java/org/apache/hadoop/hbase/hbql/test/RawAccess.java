package org.apache.hadoop.hbase.hbql.test;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 17, 2009
 * Time: 2:43:58 PM
 */
public class RawAccess {

    public static void main(String[] args) throws IOException, HPersistException {

        byte[] fbytes = Bytes.toBytes("family1");
        byte[] cbytes = Bytes.toBytes("author");

        HTable table = new HTable(new HBaseConfiguration(), "testobjects");

        for (int i = 0; i < 5; i++) {
            Put put = new Put(Bytes.toBytes("00000000" + i));
            put.add(fbytes, cbytes, Bytes.toBytes("A value for author"));
            table.put(put);
            table.flushCommits();
        }

        Scan scan = new Scan();
        scan.addColumn(fbytes, cbytes);
        ResultScanner scanner = table.getScanner(scan);

        for (Result result : scanner) {
            String val = Bytes.toString(result.getRow());
            System.out.println(val);
        }

    }
}

package com.imap4j.hbase;

import com.imap4j.hbase.hbql.HBPersistException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 17, 2009
 * Time: 9:38:45 AM
 */
public class HBaseTests {

    final static String tablename = "blogposts";

    public static void main(String[] args) throws IOException, HBPersistException {

        /*
        int cnt = 0; //10000;
        for (int i = 1; i < cnt; i++)
            insertPost("post" + System.currentTimeMillis() + "-" + i);
         */

        deleteAll(tablename);
        TestObject.main(null);

        /*
        for (int j = 1; j < 5; j++) {
            String key = "post" + j;
            Map<String, String> blogpost = HBaseConnector.retrievePost(key);
            for (String col : blogpost.keySet())
                System.out.println(key + ": " + blogpost.get(col) + " - " + blogpost.get(col));
        }
        */

        HBaseAdmin admin = new HBaseAdmin(new HBaseConfiguration());
        HTableDescriptor desc = admin.getTableDescriptor(tablename.getBytes());

        for (HColumnDescriptor col : desc.getFamilies())
            System.out.println("Family: " + col.getNameAsString());

    }

    public static void deleteAll(final String tablename) throws IOException {

        final HTable table = new HTable(new HBaseConfiguration(), tablename);

        final Scan scan = new Scan();
        scan.addColumn("post:".getBytes());

        ResultScanner scanner = table.getScanner(scan);
        int tot = 0;
        for (Result res : scanner) {
            String key = new String(res.getRow());
            //System.out.println("Key: " + key);
            final Delete delete = new Delete(res.getRow());
            table.delete(delete);
            tot++;
            //System.out.println(res);
            //for (byte[] b : res.keySet())
            //    System.out.println(new String(b));
        }
        System.out.println("Count: " + tot);

    }

}

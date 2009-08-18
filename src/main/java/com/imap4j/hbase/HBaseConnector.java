package com.imap4j.hbase;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scanner;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.RowResult;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 17, 2009
 * Time: 9:38:45 AM
 */
public class HBaseConnector {

    final static String tablename = "blogposts";

    public static void insertPost(String val) throws IOException {

        BatchUpdate upd = new BatchUpdate(val);
        upd.put("post:title", ("This is a title " + val).getBytes());
        upd.put("post:author", ("This is a author " + val).getBytes());
        upd.put("post:body", ("This is a body " + val).getBytes());
        upd.put("image:header", ("This is a header " + val).getBytes());
        upd.put("image:bodyimage", ("This is a image " + val).getBytes());

        final HTable table = new HTable(new HBaseConfiguration(), tablename);
        table.commit(upd);

    }

    public static Map<String, String> retrievePost(String postId) throws IOException {

        final HTable table = new HTable(new HBaseConfiguration(), tablename);
        final Map<String, String> post = new HashMap<String, String>();
        final RowResult result = table.getRow(postId);

        for (byte[] column : result.keySet()) {
            String str = new String(column);
            String val = new String(result.get(column).getValue());
            post.put(str, val);
        }

        return post;
    }

    public static void main(String[] args) throws IOException {

        //for (int i = 2; i < 5; i++)
        //    insertPost("post" + i);

        /*
        for (int j = 1; j < 5; j++) {
            String key = "post" + j;
            Map<String, String> blogpost = HBaseConnector.retrievePost(key);
            for (String col : blogpost.keySet())
                System.out.println(key + ": " + blogpost.get(col) + " - " + blogpost.get(col));
        }
        */

        HBaseAdmin admin = new HBaseAdmin(new HBaseConfiguration());
        HTableDescriptor desc = admin.getTableDescriptor(tablename);

        for (HColumnDescriptor col : desc.getFamilies())
            System.out.println("Family: " + col.getNameAsString());

        final HTable table = new HTable(new HBaseConfiguration(), tablename);

        byte[][] start = table.getStartKeys();
        byte[][] end = table.getEndKeys();
        String s1 = new String(start[0]);
        String s2 = new String(end[0]);
        int g = 9;

        Scanner scanner = table.getScanner(new String[]{"post:"});
        for (RowResult res : scanner) {
            System.out.println(res);
            System.out.println("Key: " + new String(res.getRow()));
            for (byte[] b : res.keySet())
                System.out.println(new String(b));
        }

    }

}

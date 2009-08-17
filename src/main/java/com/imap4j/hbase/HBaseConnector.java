package com.imap4j.hbase;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
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

    public static Map retrievePost(String postId) throws IOException {

        final HTable table = new HTable(new HBaseConfiguration(), "blogposts");
        final Map<String, String> post = new HashMap<String, String>();
        final RowResult result = table.getRow(postId);

        for (byte[] column : result.keySet())
            post.put(new String(column), new String(result.get(column).getValue()));

        return post;
    }

    public static void main(String[] args) throws IOException {
        Map blogpost = HBaseConnector.retrievePost("post1");
        for (int i = 0; i < 10000; i++)
            System.out.println(blogpost.get("post:title") + " - " + blogpost.get("post:author"));
    }

}

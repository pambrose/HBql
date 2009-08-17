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
        HTable table = new HTable(new HBaseConfiguration(), "blogposts");
        Map<String, String> post = new HashMap<String, String>();

        RowResult result = table.getRow(postId);

        for (byte[] column : result.keySet()) {
            post.put(new String(column), new String(result.get(column).getValue()));
        }
        return post;
    }

    public static void main(String[] args) throws IOException {
        Map blogpost = HBaseConnector.retrievePost("post1");
        System.out.println(blogpost.get("post:title"));
        System.out.println(blogpost.get("post:author"));
    }

}

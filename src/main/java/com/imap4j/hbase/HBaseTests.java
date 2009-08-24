package com.imap4j.hbase;

import com.imap4j.hbase.hbql.HBPersistException;

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

        TestObject.main(null);

        /*
        for (int j = 1; j < 5; j++) {
            String key = "post" + j;
            Map<String, String> blogpost = HBaseConnector.retrievePost(key);
            for (String col : blogpost.keySet())
                System.out.println(key + ": " + blogpost.get(col) + " - " + blogpost.get(col));
        }
        */

    }

}

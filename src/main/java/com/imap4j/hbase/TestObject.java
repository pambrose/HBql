package com.imap4j.hbase;

import com.google.common.collect.Maps;
import com.imap4j.hbase.hbql.Column;
import com.imap4j.hbase.hbql.HBql;
import com.imap4j.hbase.hbql.PersistException;
import com.imap4j.hbase.hbql.Persistable;
import com.imap4j.hbase.hbql.Query;
import com.imap4j.hbase.hbql.QueryListenerAdapter;
import com.imap4j.hbase.hbql.Table;
import com.imap4j.hbase.hbql.Transaction;

import java.io.IOException;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 19, 2009
 * Time: 4:39:06 PM
 */
@Table(name = "blogposts")
public class TestObject implements Persistable {

    final String family1 = "post";
    final String family2 = "image";

    @Column(key = true)
    public String keyval;

    @Column(family = family1)
    public int intValue = -999;

    @Column(family = family1)
    public String title = "A title value";

    @Column(family = family1, column = "author")
    public String author = "An author value";

    @Column(family = family2, getter = "getHeaderBytes", setter = "setHeaderBytes")
    String header = "A header value";

    @Column(family = family2, column = "bodyimage")
    String bodyimage = "A bodyimage value";

    @Column(family = family2)
    int[] array1 = {1, 2, 3};

    @Column(family = family2)
    String[] array2 = {"val1", "val2", "val3"};

    @Column(family = family2, mapKeysAsColumns = true)
    Map<String, String> mapval1 = Maps.newHashMap();

    public TestObject() {
        this.keyval = "New Val-" + System.nanoTime();

        mapval1.put("key1", "val1");
        mapval1.put("key2", "val2");
    }

    public byte[] getHeaderBytes() {
        return this.header.getBytes();
    }

    public void setHeaderBytes(byte[] val) {
        this.header = new String(val);
    }

    public static void main(String[] args) throws IOException, PersistException {

        Transaction tx = new Transaction();

        int cnt = 0;
        for (int i = 0; i < cnt; i++) {
            TestObject obj = new TestObject();
            tx.insert(obj);
        }

        tx.commit();

        HBql.exec("set classpath = com.imap4j.hbsql:com.imap4j.hbase");

        Query<TestObject> q =
                new Query<TestObject>("select author, title from TestObject",
                                      new QueryListenerAdapter<TestObject>() {
                                          public void onEachRow(final TestObject val) throws PersistException {
                                              System.out.println("Key: " + val.keyval
                                                                 + " - " + val.author
                                                                 + " - " + val.title);
                                          }
                                      });

        q.execute();

    }
}

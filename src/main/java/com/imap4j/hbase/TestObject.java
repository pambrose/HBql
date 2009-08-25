package com.imap4j.hbase;

import com.google.common.collect.Maps;
import com.imap4j.hbase.hbql.HBColumn;
import com.imap4j.hbase.hbql.HBFamily;
import com.imap4j.hbase.hbql.HBPersistException;
import com.imap4j.hbase.hbql.HBPersistable;
import com.imap4j.hbase.hbql.HBQuery;
import com.imap4j.hbase.hbql.HBQueryListenerAdapter;
import com.imap4j.hbase.hbql.HBTable;
import com.imap4j.hbase.hbql.HBql;

import java.io.IOException;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 19, 2009
 * Time: 4:39:06 PM
 */
@HBTable(name = "testobjects",
         families = {
                 @HBFamily(name = "family1", maxVersions = 10),
                 @HBFamily(name = "family2"),
                 @HBFamily(name = "family3", maxVersions = 5)
         })
public class TestObject implements HBPersistable {

    private enum TestEnum {
        RED, BLUE, BLACK, ORANGE
    }

    @HBColumn(key = true)
    private String keyval;

    @HBColumn(family = "family1")
    private TestEnum enumValue = TestEnum.BLUE;

    @HBColumn(family = "family1")
    private int intValue = -999;

    @HBColumn(family = "family1")
    private String title = "A title value";

    @HBColumn(family = "family1", column = "author")
    private String author = "An author value";

    @HBColumn(family = "family2", getter = "getHeaderBytes", setter = "setHeaderBytes")
    private String header = "A header value";

    @HBColumn(family = "family2", column = "bodyimage")
    private String bodyimage = "A bodyimage value";

    @HBColumn(family = "family2")
    private int[] array1 = {1, 2, 3};

    @HBColumn(family = "family2")
    private String[] array2 = {"val1", "val2", "val3"};

    @HBColumn(family = "family3", mapKeysAsColumns = true)
    private Map<String, String> mapval1 = Maps.newHashMap();

    public TestObject() {
        this.keyval = "Val-" + System.nanoTime();

        mapval1.put("key1", "val1");
        mapval1.put("key2", "val2");
    }

    public byte[] getHeaderBytes() {
        return this.header.getBytes();
    }

    public void setHeaderBytes(byte[] val) {
        this.header = new String(val);
    }

    public static void main(String[] args) throws IOException, HBPersistException {

        HBql.exec("set classpath com.imap4j.hbsql:com.imap4j.hbase");

        HBql.exec("create table TestObject");

        /*
        HBql.exec("delete from TestObject");

        final HBTransaction tx = new HBTransaction();
        int cnt = 2;
        for (int i = 0; i < cnt; i++) {
            TestObject obj = new TestObject();
            tx.insert(obj);
        }

        tx.commit();
        */

/*
        HBQuery<TestObject> q1 =
                new HBQuery<TestObject>("select mapval1, author, title from TestObject",
                                        new HBQueryListenerAdapter<TestObject>() {
                                            public void onEachRow(final TestObject val) throws HBPersistException {
                                                System.out.println("Values: " + val.keyval
                                                                   + " - " + val.author
                                                                   + " - " + val.title);
                                            }
                                        });

        q1.execute();
*/
        HBQuery<TestObject> q2 =
                new HBQuery<TestObject>("select * from TestObject",
                                        new HBQueryListenerAdapter<TestObject>() {
                                            public void onEachRow(final TestObject val) throws HBPersistException {
                                                System.out.println("Values: " + val.keyval
                                                                   + " - " + val.author
                                                                   + " - " + val.title);
                                            }
                                        });

        q2.execute();

    }
}

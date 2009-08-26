package com.imap4j.hbase;

import com.google.common.collect.Maps;
import com.imap4j.hbase.hql.HColumn;
import com.imap4j.hbase.hql.HFamily;
import com.imap4j.hbase.hql.HPersistException;
import com.imap4j.hbase.hql.HPersistable;
import com.imap4j.hbase.hql.HQuery;
import com.imap4j.hbase.hql.HQueryListenerAdapter;
import com.imap4j.hbase.hql.HTable;
import com.imap4j.hbase.hql.HTransaction;
import com.imap4j.hbase.hql.Hql;

import java.io.IOException;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 19, 2009
 * Time: 4:39:06 PM
 */
@HTable(name = "testobjects",
        families = {
                @HFamily(name = "family1", maxVersions = 10),
                @HFamily(name = "family2"),
                @HFamily(name = "family3", maxVersions = 5)
        })
public class TestObject implements HPersistable {

    private enum TestEnum {
        RED, BLUE, BLACK, ORANGE
    }

    @HColumn(key = true)
    private String keyval;

    @HColumn(family = "family1")
    private TestEnum enumValue = TestEnum.BLUE;

    @HColumn(family = "family1")
    private int intValue = -1;

    @HColumn(family = "family1")
    private String strValue = "";

    @HColumn(family = "family1")
    private String title = "A title value";

    @HColumn(family = "family1", column = "author")
    private String author = "An author value";

    @HColumn(family = "family2", getter = "getHeaderBytes", setter = "setHeaderBytes")
    private String header = "A header value";

    @HColumn(family = "family2", column = "bodyimage")
    private String bodyimage = "A bodyimage value";

    @HColumn(family = "family2")
    private int[] array1 = {1, 2, 3};

    @HColumn(family = "family2")
    private String[] array2 = {"val1", "val2", "val3"};

    @HColumn(family = "family3", mapKeysAsColumns = true)
    private Map<String, String> mapval1 = Maps.newHashMap();

    @HColumn(family = "family3", mapKeysAsColumns = false)
    private Map<String, String> mapval2 = Maps.newHashMap();

    public TestObject() {
    }

    public TestObject(int val) {
        this.keyval = "Val: " + System.nanoTime();

        strValue = "v" + val;

        mapval1.put("key1", "val1");
        mapval1.put("key2", "val2");

        mapval2.put("key3", "val3");
        mapval2.put("key4", "val4");
    }

    public byte[] getHeaderBytes() {
        return this.header.getBytes();
    }

    public void setHeaderBytes(byte[] val) {
        this.header = new String(val);
    }

    public static void main(String[] args) throws IOException, HPersistException {

        Hql.Results results;

        results = Hql.exec("set classpath com.imap4j.hql:com.imap4j.hbase");
        System.out.println(results.getOutput());

        //results = Hql.exec("delete from TestObject");
        System.out.println(results.getOutput());

        //results = Hql.exec("create table TestObject");
        System.out.println(results.getOutput());

        //results = Hql.exec("show tables");
        System.out.println(results.getOutput());

        //results = Hql.exec("describe table TestObject");
        System.out.println(results.getOutput());

        final HTransaction tx = new HTransaction();
        int cnt = 0;
        for (int i = 0; i < cnt; i++) {
            TestObject obj = new TestObject(i);
            tx.insert(obj);
        }

        tx.commit();

        /*
        HQuery<TestObject> q1 =
                new HQuery<TestObject>("select mapval1, author, title from TestObject",
                                       new HQueryListenerAdapter<TestObject>() {
                                           public void onEachRow(final TestObject val) throws HPersistException {
                                               System.out.println("Values: " + val.keyval
                                                                  + " - " + val.author
                                                                  + " - " + val.title);
                                           }
                                       });

        q1.execute();
        */

        long start = System.currentTimeMillis();
        HQuery<TestObject> q2 =
                new HQuery<TestObject>("select * from TestObject WHERE strValue = 'v19' OR strValue IN ('v2', 'v0', 'v999')",
                                       new HQueryListenerAdapter<TestObject>() {
                                           public void onEachRow(final TestObject val) throws HPersistException {
                                               System.out.println("Values: " + val.keyval
                                                                  + " - " + val.strValue
                                                                  + " - " + val.author
                                                                  + " - " + val.title);
                                           }
                                       });

        q2.execute();

        System.out.println("Time = " + (System.currentTimeMillis() - start));

    }
}

package com.imap4j.hbase;

import com.google.common.collect.Maps;
import com.imap4j.hbase.hbase.HBql;
import com.imap4j.hbase.hbase.HColumn;
import com.imap4j.hbase.hbase.HColumnVersion;
import com.imap4j.hbase.hbase.HFamily;
import com.imap4j.hbase.hbase.HPersistException;
import com.imap4j.hbase.hbase.HPersistable;
import com.imap4j.hbase.hbase.HQuery;
import com.imap4j.hbase.hbase.HQueryListenerAdapter;
import com.imap4j.hbase.hbase.HTable;
import com.imap4j.hbase.hbase.HTransaction;
import com.imap4j.hbase.hbql.schema.HUtil;

import java.io.IOException;
import java.util.Date;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

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

    @HColumnVersion(instance = "title")
    private NavigableMap<Long, String> titleVersions = new TreeMap<Long, String>();

    @HColumn(family = "family1", column = "author")
    private String author = "An author value";

    @HColumnVersion(instance = "author")
    private NavigableMap<Long, String> authorVersions;

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

    public TestObject(int val) throws HPersistException {
        this.keyval = HUtil.getZeroPaddedNumber(val, 6);

        strValue = "v" + val;

        mapval1.put("key1", "val1");
        mapval1.put("key2", "val2");

        mapval2.put("key3", "val3");
        mapval2.put("key4", "val4");

        author += "-" + val + System.nanoTime();
        header += "-" + val;
        title += "-" + val;
    }

    public byte[] getHeaderBytes() {
        return this.header.getBytes();
    }

    public void setHeaderBytes(byte[] val) {
        this.header = new String(val);
    }

    public static void main(String[] args) throws IOException, HPersistException {

        HBql.Results results;

        results = HBql.exec("set packagepath 'com.imap4j.hbql:com.imap4j.hbase'");
        System.out.println(results.getOutput());

        //results = HBql.exec("delete from TestObject with  client filter true");
        //System.out.println(results.getOutput());

        //results = HBql.exec("create table TestObject");
        //System.out.println(results.getOutput());

        /*
        results = HBql.exec("show tables");
        System.out.println(results.getOutput());

        results = HBql.exec("describe table TestObject");
        System.out.println(results.getOutput());
        */

        final HTransaction tx = new HTransaction();
        int cnt = 0;
        for (int i = 0; i < cnt; i++) {
            TestObject obj = new TestObject(i);
            tx.insert(obj);
        }

        tx.commit();

        long start = System.currentTimeMillis();
        HQuery<TestObject> q2 =
                new HQuery<TestObject>("select * authorVersions from TestObject WITH "
                                       + "KEYS  '000002' : '000005','000007':LAST "
                                       + "TIME RANGE NOW : TOMORROW"
                                       + "VERSIONS 5 "
                        ,
                                       new HQueryListenerAdapter<TestObject>() {
                                           public void onEachRow(final TestObject val) throws HPersistException {

                                               System.out.println("Current Values: " + val.keyval
                                                                  + " - " + val.strValue
                                                                  + " - " + val.author
                                                                  + " - " + val.title);

                                               System.out.println("Historicals");

                                               if (val.authorVersions != null)
                                                   for (final Long key : val.authorVersions.keySet())
                                                       System.out.println(new Date(key) + " - "
                                                                          + val.authorVersions.get(key));

                                               if (val.titleVersions != null)
                                                   for (final Long key : val.titleVersions.keySet())
                                                       System.out.println(new Date(key) + " - "
                                                                          + val.titleVersions.get(key));
                                           }
                                       });

        q2.execute();

        System.out.println("Time = " + (System.currentTimeMillis() - start));

    }
}

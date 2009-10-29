package org.apache.expreval.examples;

import org.apache.expreval.client.HBqlException;
import org.apache.expreval.util.HUtil;
import org.apache.expreval.util.Maps;
import org.apache.hadoop.hbase.contrib.hbql.client.HBatch;
import org.apache.hadoop.hbase.contrib.hbql.client.HColumn;
import org.apache.hadoop.hbase.contrib.hbql.client.HColumnVersionMap;
import org.apache.hadoop.hbase.contrib.hbql.client.HConnection;
import org.apache.hadoop.hbase.contrib.hbql.client.HConnectionManager;
import org.apache.hadoop.hbase.contrib.hbql.client.HFamily;
import org.apache.hadoop.hbase.contrib.hbql.client.HQuery;
import org.apache.hadoop.hbase.contrib.hbql.client.HResults;
import org.apache.hadoop.hbase.contrib.hbql.client.HTable;

import java.io.IOException;
import java.util.Date;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

public class AnnotationExample {

    @HTable(name = "testobjects",
            families = {
                    @HFamily(name = "family1", maxVersions = 10),
                    @HFamily(name = "family2"),
                    @HFamily(name = "family3", maxVersions = 5)
            })
    public static class TestObject {

        private enum TestEnum {
            RED, BLUE, BLACK, ORANGE
        }

        @HColumn(key = true)
        public String keyval;

        @HColumn(family = "family1")
        public TestEnum enumValue = TestEnum.BLUE;

        @HColumn(family = "family1")
        public int intValue = -1;

        @HColumn(family = "family1")
        public String strValue = "";

        @HColumn(family = "family1")
        public String title = "";

        @HColumnVersionMap(instance = "title")
        public NavigableMap<Long, String> titles = new TreeMap<Long, String>();

        @HColumn(family = "family1", column = "author")
        public String author = "";

        @HColumnVersionMap(instance = "author")
        public NavigableMap<Long, String> authorVersions;

        @HColumn(family = "family3", familyDefault = true)
        public Map<String, byte[]> family1Default = Maps.newHashMap();

        @HColumnVersionMap(instance = "family1Default")
        public Map<String, NavigableMap<Long, byte[]>> family1DefaultVersions;

        @HColumn(family = "family2", getter = "getHeaderBytes", setter = "setHeaderBytes")
        public String header = "A header value";

        @HColumn(family = "family2", column = "bodyimage")
        public String bodyimage = "A bodyimage value";

        @HColumn(family = "family2")
        public int[] array1 = {1, 2, 3};

        @HColumn(family = "family2")
        public String[] array2 = {"val1", "val2", "val3"};

        @HColumn(family = "family3", mapKeysAsColumns = true)
        public Map<String, String> mapval1 = Maps.newHashMap();

        @HColumnVersionMap(instance = "mapval1")
        public Map<String, NavigableMap<Long, String>> mapval1Versions;

        @HColumn(family = "family3", mapKeysAsColumns = false)
        public Map<String, String> mapval2 = Maps.newHashMap();

        public TestObject() {
        }

        public TestObject(int val) throws HBqlException {
            this.keyval = HUtil.getZeroPaddedNumber(val, 6);

            this.title = "A title value";
            this.author = "An author value";
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
    }

    public static void main(String[] args) throws IOException, HBqlException {

        HConnection conn = HConnectionManager.newHConnection();

        /*
        if (conn.tableExists("TestObject")) {
            System.out.println(conn.execute("disable table TestObject"));
            System.out.println(conn.execute("drop table TestObject"));
        }
        */

        if (!conn.tableExists("TestObject")) {
            System.out.println(conn.execute("create table with schema TestObject"));

            final HBatch batch = new HBatch();
            for (int i = 0; i < 10; i++)
                batch.insert(new TestObject(i));

            conn.apply(batch);
        }

        final String query2 = "SELECT title, titles, author, authorVersions "
                              + "FROM TestObject "
                              + "WITH "
                              + "KEYS '0000000002' TO '0000000003', '0000000007' TO '0000000008' "
                              + "TIME RANGE NOW()-DAY(25) TO NOW()+DAY(1) "
                              + "VERSIONS 3 "
                              //+ "SERVER FILTER WHERE author LIKE '.*val.*' OR LENGTH(author) > 4 "
                              + "CLIENT FILTER WHERE author LIKE '.*val.*' OR LENGTH(author) > 4";

        HQuery<TestObject> q2 = conn.newHQuery(query2);
        HResults<TestObject> results2 = q2.getResults();

        for (TestObject val2 : results2) {
            System.out.println("Current Values: " + val2.keyval + " - " + val2.author + " - " + val2.title);

            System.out.println("Historicals");
            if (val2.authorVersions != null)
                for (final Long key : val2.authorVersions.keySet())
                    System.out.println(new Date(key) + " - " + val2.authorVersions.get(key));

            if (val2.titles != null)
                for (final Long key : val2.titles.keySet())
                    System.out.println(new Date(key) + " - " + val2.titles.get(key));
        }

        results2.close();
    }
}

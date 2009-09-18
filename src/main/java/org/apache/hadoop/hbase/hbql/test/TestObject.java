package org.apache.hadoop.hbase.hbql.test;

import org.apache.hadoop.hbase.hbql.client.HColumn;
import org.apache.hadoop.hbase.hbql.client.HColumnVersionMap;
import org.apache.hadoop.hbase.hbql.client.HFamily;
import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.client.HTable;
import org.apache.hadoop.hbase.hbql.query.schema.HUtil;
import org.apache.hadoop.hbase.hbql.query.util.Maps;

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
public class TestObject {

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

    @HColumn(family = "family3", mapKeysAsColumns = false)
    public Map<String, String> mapval2 = Maps.newHashMap();

    public TestObject() {
    }

    public TestObject(int val) throws HPersistException {
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

package org.apache.hadoop.hbase.hbql;

import org.apache.hadoop.hbase.hbql.client.HColumn;
import org.apache.hadoop.hbase.hbql.client.HFamily;
import org.apache.hadoop.hbase.hbql.client.HTable;

@HTable(name = "alltypes",
        families = {
                @HFamily(name = "family1", maxVersions = 10),
                @HFamily(name = "family2"),
                @HFamily(name = "family3", maxVersions = 5)
        })
public class AnnotatedAllTypes {

    @HColumn(key = true)
    private String keyval = null;

    @HColumn(family = "family1")
    private int intValue = -1;

    @HColumn(family = "family1")
    private String stringValue = "";

    public AnnotatedAllTypes() {
    }

    public AnnotatedAllTypes(final String keyval, final int intValue, final String stringValue) {
        this.keyval = keyval;
        this.intValue = intValue;
        this.stringValue = stringValue;
    }
}

package org.apache.hadoop.hbase.contrib.hbql;

import org.apache.hadoop.hbase.contrib.hbql.client.Column;
import org.apache.hadoop.hbase.contrib.hbql.client.Family;
import org.apache.hadoop.hbase.contrib.hbql.client.Table;

@Table(name = "alltypes",
       families = {
               @Family(name = "family1", maxVersions = 10),
               @Family(name = "family2"),
               @Family(name = "family3", maxVersions = 5)
       })
public class AnnotatedAllTypes {

    @Column(key = true)
    private String keyval = null;

    @Column(family = "family1")
    private int intValue = -1;

    @Column(family = "family1")
    private String stringValue = "";

    public AnnotatedAllTypes() {
    }

    public AnnotatedAllTypes(final String keyval, final int intValue, final String stringValue) {
        this.keyval = keyval;
        this.intValue = intValue;
        this.stringValue = stringValue;
    }
}

package org.apache.hadoop.hbase.contrib.hbql;

import org.apache.hadoop.hbase.contrib.hbql.client.Column;
import org.apache.hadoop.hbase.contrib.hbql.client.Family;
import org.apache.hadoop.hbase.contrib.hbql.client.Table;

import java.io.Serializable;

// START SNIPPET: annotatedExample1

@Table(name = "example2",
       families = {
               @Family(name = "f1", maxVersions = 10)
       })
public class AnnotatedExample implements Serializable {

    @Column(key = true)
    public String keyval = null;

    @Column(family = "f1")
    public String val1;

    @Column(family = "f1")
    public int val2;

    @Column(family = "f1")
    public String val3 = "This is a default value";
}

// START SNIPPET: annotatedExample1

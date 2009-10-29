package org.apache.hadoop.hbase.contrib.hbql.util;

public class Counter {
    int count = 0;

    public void increment() {
        count++;
    }

    public int getCount() {
        return this.count;
    }

    public void reset() {
        this.count = 0;
    }
}


package org.apache.hadoop.hbase.hbql.util;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 10, 2009
 * Time: 9:40:28 PM
 */
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


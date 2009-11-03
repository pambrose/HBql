package org.apache.hadoop.hbase.contrib.hbql.client;

public class HBqlException extends java.lang.Exception {

    public HBqlException(final String s) {
        super(s);
    }

    public HBqlException(final String s, final Throwable throwable) {
        super(s, throwable);
    }
}

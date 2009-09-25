package org.apache.hadoop.hbase.hbql.client;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 19, 2009
 * Time: 5:40:42 PM
 */
public class HBqlException extends java.lang.Exception {

    public HBqlException(final String s) {
        super(s);
    }

    public HBqlException(final String s, final Throwable throwable) {
        super(s, throwable);
    }

    public HBqlException(final Throwable throwable) {
        super(throwable);
    }
}

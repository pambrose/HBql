package org.apache.hadoop.hbase.hbql.client;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 19, 2009
 * Time: 5:40:42 PM
 */
public class HPersistException extends java.lang.Exception {

    public HPersistException(final String s) {
        super(s);
    }

    public HPersistException(final String s, final Throwable throwable) {
        super(s, throwable);
    }

    public HPersistException(final Throwable throwable) {
        super(throwable);
    }
}

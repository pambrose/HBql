package com.imap4j.hbase.hbsql;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 19, 2009
 * Time: 5:40:42 PM
 */
public class PersistException extends java.lang.Exception {

    public PersistException(final String s) {
        super(s);
    }

    public PersistException(final String s, final Throwable throwable) {
        super(s, throwable);
    }

    public PersistException(final Throwable throwable) {
        super(throwable);
    }
}

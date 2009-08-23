package com.imap4j.hbase.hbql;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 19, 2009
 * Time: 5:40:42 PM
 */
public class HBPersistException extends java.lang.Exception {

    public HBPersistException(final String s) {
        super(s);
    }

    public HBPersistException(final String s, final Throwable throwable) {
        super(s, throwable);
    }

    public HBPersistException(final Throwable throwable) {
        super(throwable);
    }
}

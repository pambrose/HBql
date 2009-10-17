package org.apache.hadoop.hbase.hbql.client;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Oct 16, 2009
 * Time: 11:25:11 PM
 */
public class InternalErrorException extends HBqlException {

    public InternalErrorException() {
        super("Internal error");
    }

    public InternalErrorException(final String s) {
        super("Internal error: " + s);
    }
}

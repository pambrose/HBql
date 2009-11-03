package org.apache.expreval.client;

import org.apache.hadoop.hbase.contrib.hbql.client.HBqlException;

public class InternalErrorException extends HBqlException {

    public InternalErrorException() {
        super("Internal error");
    }

    public InternalErrorException(final String s) {
        super("Internal error: " + s);
    }
}

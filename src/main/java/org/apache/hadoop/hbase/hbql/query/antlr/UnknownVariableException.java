package org.apache.hadoop.hbase.hbql.query.antlr;

public class UnknownVariableException extends RuntimeException {
    public UnknownVariableException(final String s) {
        super(s);
    }
}

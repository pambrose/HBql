package org.apache.hadoop.hbase.contrib.hbql.client;

import org.apache.expreval.hbql.impl.ConnectionImpl;
import org.apache.expreval.statement.ConnectionStatement;

import java.io.IOException;

public interface PreparedStatement extends ConnectionStatement {

    int setParameter(final String name, final Object val) throws HBqlException;

    HOutput execute() throws HBqlException, IOException;

    void reset();

    void validate(final ConnectionImpl connection) throws HBqlException;
}

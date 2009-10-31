package org.apache.hadoop.hbase.contrib.hbql.client;

import org.apache.expreval.client.HBqlException;
import org.apache.hadoop.hbase.contrib.hbql.impl.ConnectionImpl;
import org.apache.hadoop.hbase.contrib.hbql.statement.ConnectionStatement;

import java.io.IOException;

public interface PreparedStatement extends ConnectionStatement {

    int setParameter(final String name, final Object val) throws HBqlException;

    Output execute() throws HBqlException, IOException;

    void reset();

    void validate(final ConnectionImpl connection) throws HBqlException;
}

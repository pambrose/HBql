package org.apache.hadoop.hbase.hbql.client;

import org.apache.hadoop.hbase.hbql.query.impl.hbase.ConnectionImpl;
import org.apache.hadoop.hbase.hbql.stmt.ConnectionStatement;

import java.io.IOException;

public interface PreparedStatement extends ConnectionStatement {

    int setParameter(final String name, final Object val) throws HBqlException;

    HOutput execute() throws HBqlException, IOException;

    void reset();

    void validate(final ConnectionImpl connection) throws HBqlException;
}

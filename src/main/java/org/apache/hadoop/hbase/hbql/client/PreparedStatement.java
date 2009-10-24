package org.apache.hadoop.hbase.hbql.client;

import org.apache.hadoop.hbase.hbql.query.impl.hbase.ConnectionImpl;

import java.io.IOException;

public interface PreparedStatement {

    int setParameter(final String name, final Object val) throws HBqlException;

    HOutput execute() throws HBqlException, IOException;

    void setConnection(ConnectionImpl connection) throws HBqlException;

    void validate() throws HBqlException;
}

package org.apache.hadoop.hbase.contrib.hbql.client;

import org.apache.expreval.util.Maps;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.contrib.hbql.impl.ConnectionImpl;

import java.util.Map;

public class ConnectionManager {

    private static Map<String, Connection> connectionMap = Maps.newHashMap();

    public static Connection newHConnection() {
        return newHConnection(null, null);
    }

    public static Connection newHConnection(final HBaseConfiguration config) {
        return newHConnection(null, config);
    }

    public static synchronized Connection newHConnection(final String name) {
        return newHConnection(name, null);
    }

    public static synchronized Connection newHConnection(final String name, final HBaseConfiguration config) {
        final ConnectionImpl conn = new ConnectionImpl(name, config);

        if (conn.getName() != null)
            ConnectionManager.getConnectionMap().put(conn.getName(), conn);

        return conn;
    }

    public static Connection getConnection(final String name) {
        return ConnectionManager.getConnectionMap().get(name);
    }

    private static Map<String, Connection> getConnectionMap() {
        return ConnectionManager.connectionMap;
    }
}
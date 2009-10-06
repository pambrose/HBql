package org.apache.hadoop.hbase.hbql.client;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.hbql.query.client.ConnectionImpl;

import java.util.Map;

public class HConnectionManager {

    private static Map<String, HConnection> connectionMap;

    public static HConnection newHConnection() {
        return newHConnection(null, null);
    }

    public static HConnection newHConnection(final HBaseConfiguration config) {
        return newHConnection(null, config);
    }

    public static synchronized HConnection newHConnection(final String name) {
        return newHConnection(name, null);
    }

    public static synchronized HConnection newHConnection(final String name, final HBaseConfiguration config) {
        final ConnectionImpl conn = new ConnectionImpl(name, config);

        if (conn.getName() != null)
            HConnectionManager.connectionMap.put(conn.getName(), conn);

        return conn;
    }

    public static HConnection getHConnection(final String name) {
        return connectionMap.get(name);
    }

}
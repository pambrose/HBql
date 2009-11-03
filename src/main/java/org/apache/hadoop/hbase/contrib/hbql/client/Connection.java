package org.apache.hadoop.hbase.contrib.hbql.client;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;

import java.io.IOException;
import java.util.Set;

public interface Connection {

    String getName();

    HBaseConfiguration getConfig();

    <T> Query<T> newQuery(String query) throws IOException, HBqlException;

    Output execute(String str) throws HBqlException, IOException;

    PreparedStatement prepare(String str) throws HBqlException;

    org.apache.hadoop.hbase.client.HTable getHTable(String tableName) throws IOException;

    boolean tableExists(String tableName) throws MasterNotRunningException;

    boolean tableEnabled(String tableName) throws IOException;

    void dropTable(String tableName) throws IOException;

    void disableTable(String tableName) throws IOException;

    void enableTable(String tableName) throws IOException;

    Set<String> getTableNames() throws IOException;

    Set<String> getFamilyNames(String tableName) throws HBqlException;

    void apply(Batch batch) throws IOException;
}

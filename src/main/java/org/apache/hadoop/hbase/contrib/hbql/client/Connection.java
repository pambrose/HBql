package org.apache.hadoop.hbase.contrib.hbql.client;

import org.apache.expreval.client.HBqlException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;

import java.io.IOException;
import java.util.List;
import java.util.Set;

public interface Connection {

    <T> Query<T> newHQuery(String query) throws IOException, HBqlException;

    String getName();

    HBaseConfiguration getConfig();

    org.apache.hadoop.hbase.client.HTable getHTable(String tableName) throws IOException;

    boolean tableExists(String tableName) throws MasterNotRunningException;

    boolean tableEnabled(String tableName) throws IOException;

    void dropTable(String tableName) throws IOException;

    void disableTable(String tableName) throws IOException;

    List<String> getTableList() throws IOException;

    Set<String> getFamilyList(String tableName) throws HBqlException;

    Output execute(String str) throws HBqlException, IOException;

    PreparedStatement prepare(String str) throws HBqlException;

    void apply(Batch batch) throws IOException;
}

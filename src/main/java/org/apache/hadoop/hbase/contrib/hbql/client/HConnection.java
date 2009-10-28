package org.apache.hadoop.hbase.contrib.hbql.client;

import org.apache.expreval.client.HBqlException;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.io.IOException;
import java.util.List;
import java.util.Set;

public interface HConnection {

    <T> HQuery<T> newHQuery(String query) throws IOException, HBqlException;

    String getName();

    HBaseConfiguration getConfig();

    org.apache.hadoop.hbase.client.HTable getHTable(String tableName) throws IOException;

    boolean tableExists(String tableName) throws IOException, HBqlException;

    boolean tableEnabled(String tableName) throws IOException, HBqlException;

    List<String> getTableList() throws IOException;

    Set<String> getFamilyList(String tableName) throws HBqlException;

    HOutput execute(String str) throws HBqlException, IOException;

    PreparedStatement prepare(String str) throws HBqlException;

    void apply(HBatch batch) throws IOException;
}

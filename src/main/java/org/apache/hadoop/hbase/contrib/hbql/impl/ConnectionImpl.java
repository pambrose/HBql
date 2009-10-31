package org.apache.hadoop.hbase.contrib.hbql.impl;

import org.apache.expreval.client.HBqlException;
import org.apache.expreval.util.Lists;
import org.apache.expreval.util.Sets;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.contrib.hbql.client.Batch;
import org.apache.hadoop.hbase.contrib.hbql.client.Connection;
import org.apache.hadoop.hbase.contrib.hbql.client.Output;
import org.apache.hadoop.hbase.contrib.hbql.client.PreparedStatement;
import org.apache.hadoop.hbase.contrib.hbql.client.Query;
import org.apache.hadoop.hbase.contrib.hbql.parser.HBqlShell;
import org.apache.hadoop.hbase.contrib.hbql.statement.ConnectionStatement;
import org.apache.hadoop.hbase.contrib.hbql.statement.SelectStatement;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;
import java.util.Set;

public class ConnectionImpl implements Connection {

    private final HBaseConfiguration config;
    private final String name;

    public ConnectionImpl(final String name, final HBaseConfiguration config) {
        this.name = name;
        this.config = (config == null) ? new HBaseConfiguration() : config;
    }

    public <T> Query<T> newQuery(final String query) throws IOException, HBqlException {
        return new QueryImpl<T>(this, query);
    }

    public <T> Query<T> newQuery(final SelectStatement selectStatement) throws IOException, HBqlException {
        return new QueryImpl<T>(this, selectStatement);
    }

    public String getName() {
        return this.name;
    }

    public HBaseConfiguration getConfig() {
        return this.config;
    }

    public HBaseAdmin getAdmin() throws MasterNotRunningException {
        return new HBaseAdmin(this.getConfig());
    }

    public HTable getHTable(final String tableName) throws IOException {
        return new HTable(this.getConfig(), tableName);
    }

    public boolean tableExists(final String tableName) throws MasterNotRunningException {
        return this.getAdmin().tableExists(tableName);
    }

    public boolean tableEnabled(final String tableName) throws IOException {
        return this.getAdmin().isTableEnabled(tableName);
    }

    public void dropTable(final String tableName) throws IOException {
        this.getAdmin().deleteTable(tableName);
    }

    public void disableTable(final String tableName) throws IOException {
        this.getAdmin().disableTable(tableName);
    }

    public List<String> getTableList() throws IOException {
        final HBaseAdmin admin = this.getAdmin();
        final List<String> tableList = Lists.newArrayList();
        for (final HTableDescriptor table : admin.listTables())
            tableList.add(table.getNameAsString());
        return tableList;
    }

    public Set<String> getFamilyList(final String tableName) throws HBqlException {
        try {
            final HTableDescriptor table = this.getAdmin().getTableDescriptor(Bytes.toBytes(tableName));
            final Set<String> familySet = Sets.newHashSet();
            for (final HColumnDescriptor descriptor : table.getColumnFamilies())
                familySet.add(Bytes.toString(descriptor.getName()));
            return familySet;
        }
        catch (IOException e) {
            e.printStackTrace();
            throw new HBqlException(e.getMessage());
        }
    }

    public Output execute(final String str) throws HBqlException, IOException {
        final ConnectionStatement statement = HBqlShell.parseConnectionStatement(str);
        return statement.execute(this);
    }

    public PreparedStatement prepare(final String str) throws HBqlException {
        final PreparedStatement stmt = HBqlShell.parsePreparedStatement(str);
        // Need to call this here to enable setParameters
        stmt.validate(this);
        return stmt;
    }

    public void apply(final Batch batch) throws IOException {
        for (final String tableName : batch.getActionList().keySet()) {
            final HTable table = this.getHTable(tableName);
            for (final BatchAction batchAction : batch.getActionList(tableName))
                batchAction.apply(table);
            table.flushCommits();
        }
    }
}

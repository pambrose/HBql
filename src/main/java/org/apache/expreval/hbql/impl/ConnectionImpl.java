package org.apache.expreval.hbql.impl;

import org.apache.expreval.antlr.HBql;
import org.apache.expreval.statement.ConnectionStatement;
import org.apache.expreval.statement.SelectStatement;
import org.apache.expreval.util.Lists;
import org.apache.expreval.util.Sets;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.contrib.hbql.client.HBatch;
import org.apache.hadoop.hbase.contrib.hbql.client.HBatchAction;
import org.apache.hadoop.hbase.contrib.hbql.client.HBqlException;
import org.apache.hadoop.hbase.contrib.hbql.client.HConnection;
import org.apache.hadoop.hbase.contrib.hbql.client.HOutput;
import org.apache.hadoop.hbase.contrib.hbql.client.HQuery;
import org.apache.hadoop.hbase.contrib.hbql.client.PreparedStatement;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;
import java.util.Set;

public class ConnectionImpl implements HConnection {

    private final HBaseConfiguration config;
    private final String name;

    public ConnectionImpl(final String name, final HBaseConfiguration config) {
        this.name = name;
        this.config = (config == null) ? new HBaseConfiguration() : config;
    }

    public <T> HQuery<T> newHQuery(final String query) throws IOException, HBqlException {
        return new QueryImpl<T>(this, query);
    }

    public <T> HQuery<T> newHQuery(final SelectStatement selectStatement) throws IOException, HBqlException {
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

    public boolean tableExists(final String tableName) throws IOException, HBqlException {
        return this.getAdmin().tableExists(tableName);
    }

    public boolean tableEnabled(final String tableName) throws IOException, HBqlException {
        return this.getAdmin().isTableEnabled(tableName);
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

    public HOutput execute(final String str) throws HBqlException, IOException {
        final ConnectionStatement statement = HBql.parseConnectionStatement(str);
        return statement.execute(this);
    }

    public PreparedStatement prepare(final String str) throws HBqlException {
        final PreparedStatement stmt = HBql.parsePreparedStatement(str);
        // Need to call this here to enable setParameters
        stmt.validate(this);
        return stmt;
    }

    public void apply(final HBatch batch) throws IOException {
        for (final String tableName : batch.getActionList().keySet()) {
            final HTable table = this.getHTable(tableName);
            final List<HBatchAction> batchActions = batch.getActionList(tableName);
            for (HBatchAction batchAction : batchActions)
                batchAction.apply(table);
            table.flushCommits();
        }
    }
}

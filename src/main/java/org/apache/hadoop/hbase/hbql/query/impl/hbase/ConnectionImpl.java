package org.apache.hadoop.hbase.hbql.query.impl.hbase;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.hbql.client.HBatch;
import org.apache.hadoop.hbase.hbql.client.HBatchAction;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HConnection;
import org.apache.hadoop.hbase.hbql.client.HOutput;
import org.apache.hadoop.hbase.hbql.client.HQuery;
import org.apache.hadoop.hbase.hbql.query.antlr.HBql;
import org.apache.hadoop.hbase.hbql.query.cmds.ConnectionCmd;
import org.apache.hadoop.hbase.hbql.query.schema.HBaseSchema;
import org.apache.hadoop.hbase.hbql.query.util.Lists;
import org.apache.hadoop.hbase.hbql.query.util.Sets;
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

    public String getName() {
        return this.name;
    }

    public HBaseConfiguration getConfig() {
        return this.config;
    }

    public HTable getHTable(final String tableName) throws IOException {
        return new HTable(this.getConfig(), tableName);
    }

    public boolean tableExists(final String tableName) throws IOException, HBqlException {
        final HBaseSchema schema = HBaseSchema.findSchema(tableName);
        final HBaseAdmin admin = new HBaseAdmin(this.getConfig());
        return admin.tableExists(schema.getTableName());
    }

    public boolean tableEnabled(final String tableName) throws IOException, HBqlException {
        final HBaseSchema schema = HBaseSchema.findSchema(tableName);
        final HBaseAdmin admin = new HBaseAdmin(this.getConfig());
        return admin.isTableEnabled(schema.getTableName());
    }

    public List<String> getTableList() throws IOException, HBqlException {
        final HBaseAdmin admin = new HBaseAdmin(this.getConfig());
        final List<String> tableList = Lists.newArrayList();
        for (final HTableDescriptor table : admin.listTables())
            tableList.add(table.getNameAsString());
        return tableList;
    }

    public Set<String> getFamilyList(final String tableName) throws HBqlException {
        try {
            final HBaseAdmin admin = new HBaseAdmin(this.getConfig());
            final HTableDescriptor table = admin.getTableDescriptor(Bytes.toBytes(tableName));
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

        final ConnectionCmd cmd = HBql.parseCommand(str);

        if (cmd == null)
            throw new HBqlException("Error parsing: " + str);

        return cmd.execute(this);
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

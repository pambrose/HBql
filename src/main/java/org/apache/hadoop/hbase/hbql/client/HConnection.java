package org.apache.hadoop.hbase.hbql.client;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.hbql.query.antlr.cmds.ExecCmd;
import org.apache.hadoop.hbase.hbql.query.antlr.config.HBqlRule;
import org.apache.hadoop.hbase.hbql.query.schema.Schema;
import org.apache.hadoop.hbase.hbql.query.util.Maps;

import java.io.IOException;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 12, 2009
 * Time: 3:27:28 PM
 */
public class HConnection {

    private static Map<String, HConnection> connectionMap = Maps.newHashMap();

    final HBaseConfiguration config;

    final String name;

    private HConnection(final String name, final HBaseConfiguration config) {
        this.name = name;

        this.config = (config == null) ? new HBaseConfiguration() : config;

        if (this.getName() != null)
            connectionMap.put(this.getName(), this);
    }

    public static synchronized HConnection newHConnection(final String name) {
        return new HConnection(name, null);
    }

    public static synchronized HConnection newHConnection(final String name,
                                                          final HBaseConfiguration config) {
        return new HConnection(name, config);
    }

    public static HConnection newHConnection() {
        return newHConnection(null, null);
    }

    public static HConnection newHConnection(final HBaseConfiguration config) {
        return newHConnection(null, config);
    }

    public static HConnection getHConnection(final String name) {
        return connectionMap.get(name);
    }

    public <T> HQuery<T> newHQuery(final String query) throws IOException, HPersistException {
        return new HQuery<T>(this, query);
    }

    public String getName() {
        return this.name;
    }

    public HBaseConfiguration getConfig() {
        return this.config;
    }

    public HTransaction newHTransaction() {
        return new HTransaction(this);
    }

    public HTable getHTable(final String tableName) throws IOException {
        return new HTable(this.getConfig(), tableName);
    }

    public HOutput exec(final String str) throws HPersistException, IOException {

        final ExecCmd cmd = (ExecCmd)HBqlRule.EXEC.parse(str, (Schema)null);

        if (cmd == null)
            throw new HPersistException("Error parsing: " + str);

        return cmd.exec(this);
    }
}

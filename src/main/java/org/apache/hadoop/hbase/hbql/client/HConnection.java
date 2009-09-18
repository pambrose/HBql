package org.apache.hadoop.hbase.hbql.client;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.HBqlFilter;
import org.apache.hadoop.hbase.hbql.query.antlr.args.CreateArgs;
import org.apache.hadoop.hbase.hbql.query.antlr.args.DefineArgs;
import org.apache.hadoop.hbase.hbql.query.antlr.args.DeleteArgs;
import org.apache.hadoop.hbase.hbql.query.antlr.args.DescribeArgs;
import org.apache.hadoop.hbase.hbql.query.antlr.args.ExecArgs;
import org.apache.hadoop.hbase.hbql.query.antlr.args.SetArgs;
import org.apache.hadoop.hbase.hbql.query.antlr.args.ShowArgs;
import org.apache.hadoop.hbase.hbql.query.antlr.args.WhereArgs;
import org.apache.hadoop.hbase.hbql.query.antlr.config.HBqlRule;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.query.schema.AnnotationSchema;
import org.apache.hadoop.hbase.hbql.query.schema.DefinedSchema;
import org.apache.hadoop.hbase.hbql.query.schema.EnvVars;
import org.apache.hadoop.hbase.hbql.query.schema.ExprSchema;
import org.apache.hadoop.hbase.hbql.query.schema.HUtil;
import org.apache.hadoop.hbase.hbql.query.schema.VarDescAttrib;
import org.apache.hadoop.hbase.hbql.query.schema.VariableAttrib;
import org.apache.hadoop.hbase.hbql.query.util.Maps;

import java.io.IOException;
import java.util.List;
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

        final ExecArgs exec = (ExecArgs)HBqlRule.EXEC.parse(str, (ExprSchema)null);

        if (exec == null)
            throw new HPersistException("Error parsing: " + str);

        if (exec instanceof CreateArgs)
            return createCommand((CreateArgs)exec);

        if (exec instanceof DefineArgs)
            return defineCommand((DefineArgs)exec);

        if (exec instanceof DescribeArgs)
            return describeCommand((DescribeArgs)exec);

        if (exec instanceof ShowArgs)
            return showCommand((ShowArgs)exec);

        if (exec instanceof DeleteArgs)
            return deleteCommand((DeleteArgs)exec);

        if (exec instanceof SetArgs)
            return setCommand((SetArgs)exec);

        throw new HPersistException("Unknown comand: " + str);
    }

    private HOutput createCommand(final CreateArgs args) throws HPersistException, IOException {
        final HOutput retval = new HOutput();
        final AnnotationSchema schema = AnnotationSchema.getAnnotationSchema(args.getClassName());

        if (schema == null)
            throw new HPersistException("Unknown class name " + args.getClassName());

        final HTableDescriptor tableDesc = new HTableDescriptor(schema.getTableName());

        for (final HFamily family : schema.getFamilies()) {
            final HColumnDescriptor columnDesc = new HColumnDescriptor(family.name());
            if (family.maxVersions() > 0)
                columnDesc.setMaxVersions(family.maxVersions());
            tableDesc.addFamily(columnDesc);
        }

        final HBaseAdmin admin = new HBaseAdmin(this.getConfig());

        admin.createTable(tableDesc);

        retval.out.println("Table " + tableDesc.getNameAsString() + " created.");
        retval.out.flush();
        return retval;
    }

    private HOutput defineCommand(final DefineArgs args) throws HPersistException {
        final HOutput retval = new HOutput();

        final DefinedSchema schema = DefinedSchema.newDefinedSchema(args.getTableName(), args.getVarList());

        for (final VariableAttrib attrib : schema.getVariableAttribs()) {
            final VarDescAttrib vdattrib = (VarDescAttrib)attrib;
            if (attrib.getFieldType() == null)
                throw new HPersistException(args.getTableName() + " attribute " + vdattrib.getVariableName()
                                            + " has unknown type " + vdattrib.getTypeName());
        }

        retval.out.println("Table " + args.getTableName() + " defined.");
        retval.out.flush();
        return retval;
    }

    private HOutput describeCommand(final DescribeArgs args) throws IOException, HPersistException {

        final HOutput retval = new HOutput();
        final AnnotationSchema schema = AnnotationSchema.getAnnotationSchema(args.getClassName());

        if (schema == null)
            throw new HPersistException("Unknown class name " + args.getClassName());

        final HBaseAdmin admin = new HBaseAdmin(this.getConfig());
        final HTableDescriptor tableDesc = admin.getTableDescriptor(schema.getTableNameAsBytes());

        retval.out.println("Table name: " + tableDesc.getNameAsString());
        retval.out.println("Families:");
        for (final HColumnDescriptor columnDesc : tableDesc.getFamilies()) {
            retval.out.println("\t" + columnDesc.getNameAsString()
                               + " Max Verions: " + columnDesc.getMaxVersions()
                               + " TTL: " + columnDesc.getTimeToLive()
                               + " Block Size: " + columnDesc.getBlocksize()
                               + " Compression: " + columnDesc.getCompression().getName()
                               + " Compression Type: " + columnDesc.getCompressionType().getName());
        }

        retval.out.flush();
        return retval;
    }

    private HOutput showCommand(final ShowArgs args) throws IOException, HPersistException {

        final HOutput retval = new HOutput();

        final HBaseAdmin admin = new HBaseAdmin(this.getConfig());
        retval.out.println("Table names: ");
        for (final HTableDescriptor tableDesc : admin.listTables())
            retval.out.println("\t" + tableDesc.getNameAsString());

        retval.out.flush();
        return retval;
    }

    private HOutput deleteCommand(final DeleteArgs args) throws HPersistException, IOException {

        final HOutput retval = new HOutput();

        final WhereArgs where = args.getWhereExpr();

        // TODO Need to grab schema from DeleteArgs (like QueryArgs in Select)
        final AnnotationSchema schema = AnnotationSchema.getAnnotationSchema(args.getTableName());

        if (schema == null)
            throw new HPersistException("Unknown table name " + args.getTableName());

        final List<String> fieldList = schema.getFieldList();
        final HTable table = this.getHTable(schema.getTableName());
        final ExprTree clientFilter = where.getClientExprTree();
        clientFilter.setSchema(schema);
        clientFilter.optimize();
        int cnt = 0;

        final HBqlFilter serverFilter = schema.getHBqlFilter(where.getServerExprTree(),
                                                             fieldList,
                                                             where.getScanLimit());

        final List<Scan> scanList = schema.getScanList(fieldList,
                                                       where.getKeyRangeArgs(),
                                                       where.getDateRangeArgs(),
                                                       where.getVersionArgs(),
                                                       serverFilter);

        for (final Scan scan : scanList) {
            final ResultScanner resultsScanner = table.getScanner(scan);
            for (final Result result : resultsScanner) {

                final Object recordObj = schema.getObject(HUtil.ser,
                                                          schema.getFieldList(),
                                                          scan.getMaxVersions(),
                                                          result);

                if (clientFilter == null || clientFilter.evaluate(recordObj)) {
                    final Delete delete = new Delete(result.getRow());
                    table.delete(delete);
                    cnt++;
                }
            }
        }
        retval.out.println("Delete count: " + cnt);
        retval.out.flush();

        return retval;
    }

    private HOutput setCommand(final SetArgs args) throws HPersistException {

        final HOutput retval = new HOutput();
        final String var = args.getVariable();

        if (var == null)
            throw new HPersistException("Error in SET command");

        if (var.equalsIgnoreCase("packagepath")) {
            EnvVars.setPackagePath(args.getValue());
            retval.out.println("PackagePath set to " + args.getValue());
            retval.out.flush();
            return retval;
        }

        throw new HPersistException("Unknown variable: " + var);
    }
}

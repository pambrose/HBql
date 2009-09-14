package com.imap4j.hbase.hbase;

import com.imap4j.hbase.antlr.args.CreateArgs;
import com.imap4j.hbase.antlr.args.DefineArgs;
import com.imap4j.hbase.antlr.args.DeleteArgs;
import com.imap4j.hbase.antlr.args.DescribeArgs;
import com.imap4j.hbase.antlr.args.ExecArgs;
import com.imap4j.hbase.antlr.args.SetArgs;
import com.imap4j.hbase.antlr.args.ShowArgs;
import com.imap4j.hbase.antlr.config.HBqlRule;
import com.imap4j.hbase.hbql.expr.ExprTree;
import com.imap4j.hbase.hbql.schema.AnnotationSchema;
import com.imap4j.hbase.hbql.schema.DefinedSchema;
import com.imap4j.hbase.hbql.schema.EnvVars;
import com.imap4j.hbase.hbql.schema.ExprSchema;
import com.imap4j.hbase.hbql.schema.HUtil;
import com.imap4j.hbase.hbql.schema.VarDescAttrib;
import com.imap4j.hbase.hbql.schema.VariableAttrib;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import java.io.IOException;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 12, 2009
 * Time: 3:27:28 PM
 */
public class HConnection {

    final HBaseConfiguration config = new HBaseConfiguration();

    public <T> HQuery<T> newHQuery(final String query) throws IOException, HPersistException {
        return new HQuery<T>(this, query);
    }

    public HBaseConfiguration getConfig() {
        return config;
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
        final HTableDescriptor tableDesc = admin.getTableDescriptor(schema.getTableName().getBytes());

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
        final AnnotationSchema schema = AnnotationSchema.getAnnotationSchema(args.getTableName());

        if (schema == null)
            throw new HPersistException("Unknown table name " + args.getTableName());

        final List<String> fieldList = schema.getFieldList();
        final HTable table = this.getHTable(schema.getTableName());
        final ExprTree clientFilter = args.getWhereExpr().getClientFilter();
        clientFilter.setSchema(schema);
        clientFilter.optimize();
        int cnt = 0;

        final List<Scan> scanList = HUtil.getScanList(schema,
                                                      fieldList,
                                                      args.getWhereExpr().getKeyRange(),
                                                      args.getWhereExpr().getVersion(),
                                                      args.getWhereExpr().getServerFilter());

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

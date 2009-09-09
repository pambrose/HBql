package com.imap4j.hbase.hbql;

import com.imap4j.hbase.antlr.args.CreateArgs;
import com.imap4j.hbase.antlr.args.DeleteArgs;
import com.imap4j.hbase.antlr.args.DescribeArgs;
import com.imap4j.hbase.antlr.args.ExecArgs;
import com.imap4j.hbase.antlr.args.SetArgs;
import com.imap4j.hbase.antlr.args.ShowArgs;
import com.imap4j.hbase.antlr.config.HBqlRule;
import com.imap4j.hbase.hbql.expr.HBqlEvalContext;
import com.imap4j.hbase.hbql.expr.predicate.ExprEvalTree;
import com.imap4j.hbase.hbql.schema.AnnotationSchema;
import com.imap4j.hbase.hbql.schema.ExprSchema;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 21, 2009
 * Time: 1:09:48 PM
 */
public class HBql {

    public static class Results {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final PrintStream out = new PrintStream(baos);

        public String getOutput() {
            return baos.toString();
        }
    }

    public static Results exec(final String str) throws HPersistException, IOException {

        final ExecArgs exec = (ExecArgs)HBqlRule.EXEC.parse(str, (ExprSchema)null);

        if (exec instanceof CreateArgs)
            return createCommand((CreateArgs)exec);

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

    private static Results createCommand(final CreateArgs args) throws HPersistException, IOException {
        final Results retval = new Results();
        final AnnotationSchema schema = AnnotationSchema.getAnnotationSchema(args.getClassname());
        final HTableDescriptor tableDesc = new HTableDescriptor(schema.getTableName());

        for (final HFamily family : schema.getFamilies()) {
            final HColumnDescriptor columnDesc = new HColumnDescriptor(family.name());
            if (family.maxVersions() > 0)
                columnDesc.setMaxVersions(family.maxVersions());
            tableDesc.addFamily(columnDesc);
        }

        final HBaseAdmin admin = new HBaseAdmin(new HBaseConfiguration());

        admin.createTable(tableDesc);

        retval.out.println("Table " + tableDesc.getNameAsString() + " created.");
        retval.out.flush();
        return retval;
    }

    private static Results describeCommand(final DescribeArgs args) throws IOException, HPersistException {

        final Results retval = new Results();
        final AnnotationSchema schema = AnnotationSchema.getAnnotationSchema(args.getClassname());
        final HBaseAdmin admin = new HBaseAdmin(new HBaseConfiguration());
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

    private static Results showCommand(final ShowArgs args) throws IOException, HPersistException {

        final Results retval = new Results();

        final HBaseAdmin admin = new HBaseAdmin(new HBaseConfiguration());
        retval.out.println("Table names: ");
        for (final HTableDescriptor tableDesc : admin.listTables())
            retval.out.println("\t" + tableDesc.getNameAsString());

        retval.out.flush();
        return retval;
    }

    private static Results deleteCommand(final DeleteArgs args) throws HPersistException, IOException {

        final Results retval = new Results();
        final AnnotationSchema schema = AnnotationSchema.getAnnotationSchema(args.getTableName());
        final List<String> fieldList = schema.getFieldList();
        final HTable table = new HTable(new HBaseConfiguration(), schema.getTableName());
        final ExprEvalTree clientFilter = args.getWhereExpr().getClientFilterArgs();
        int cnt = 0;

        final List<Scan> scanList = HUtil.getScanList(schema, fieldList, args.getWhereExpr());
        for (final Scan scan : scanList) {
            final ResultScanner resultsScanner = table.getScanner(scan);
            for (final Result result : resultsScanner) {

                final HPersistable recordObj = HUtil.ser.getHPersistable(schema, scan, result);

                if (clientFilter == null || clientFilter.evaluate(new HBqlEvalContext(schema, recordObj))) {
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

    private static Results setCommand(final SetArgs args) throws HPersistException {

        final Results retval = new Results();
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

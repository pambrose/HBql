package com.imap4j.hbase.hql;

import com.imap4j.hbase.antlr.CreateArgs;
import com.imap4j.hbase.antlr.DeleteArgs;
import com.imap4j.hbase.antlr.DescribeArgs;
import com.imap4j.hbase.antlr.ExecArgs;
import com.imap4j.hbase.antlr.SetArgs;
import com.imap4j.hbase.antlr.config.HBqlRule;
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

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 21, 2009
 * Time: 1:09:48 PM
 */
public class Hql {

    public static class Results {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final PrintStream out = new PrintStream(baos);

        public String getOutput() {
            return baos.toString();
        }
    }

    public static Results exec(final String str) throws HPersistException, IOException {

        final ExecArgs exec = (ExecArgs)HBqlRule.EXEC.parse(str);

        if (exec instanceof CreateArgs)
            return createCommand((CreateArgs)exec);

        if (exec instanceof DescribeArgs)
            return describeCommand((DescribeArgs)exec);

        if (exec instanceof DeleteArgs)
            return deleteCommand((DeleteArgs)exec);

        if (exec instanceof SetArgs)
            return setCommand((SetArgs)exec);

        throw new HPersistException("Unknown comand");
    }

    private static Results createCommand(final CreateArgs args) throws HPersistException, IOException {
        final Results retval = new Results();
        final ClassSchema schema = ClassSchema.getClassSchema(args.getClassname());
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
        final ClassSchema schema = ClassSchema.getClassSchema(args.getClassname());
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

    private static Results deleteCommand(final DeleteArgs args) throws HPersistException, IOException {
        final Results retval = new Results();
        final ClassSchema schema = ClassSchema.getClassSchema(args.getClassname());
        final HTable table = new HTable(new HBaseConfiguration(), schema.getTableName());

        final Scan scan = new Scan();
        final ResultScanner scanner = table.getScanner(scan);
        int cnt = 0;
        for (Result res : scanner) {
            final Delete delete = new Delete(res.getRow());
            table.delete(delete);
            cnt++;
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

        if (var.equalsIgnoreCase("classpath")) {
            EnvVars.setClasspath(args.getValue());
            retval.out.println("Classpath set to " + args.getValue());
            retval.out.flush();
            return retval;
        }

        throw new HPersistException("Unknown variable: " + var);
    }

}

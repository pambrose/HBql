package com.imap4j.hbase.hbql;

import com.imap4j.hbase.antlr.CreateArgs;
import com.imap4j.hbase.antlr.DeleteArgs;
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

import java.io.IOException;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 21, 2009
 * Time: 1:09:48 PM
 */
public class HBql {

    public static void exec(final String str) throws HBPersistException, IOException {

        final ExecArgs exec = (ExecArgs)HBqlRule.EXEC.parse(str);

        if (exec instanceof DeleteArgs) {
            final DeleteArgs args = (DeleteArgs)exec;
            final ClassSchema schema = ClassSchema.getClassSchema(args.getClassname());
            deleteAll(schema.getTableName());
            return;
        }

        if (exec instanceof CreateArgs) {
            final CreateArgs args = (CreateArgs)exec;
            final ClassSchema schema = ClassSchema.getClassSchema(args.getClassname());
            createTable(schema);
            return;
        }

        if (exec instanceof SetArgs) {

            final SetArgs args = (SetArgs)exec;

            final String var = args.getVariable();
            if (var == null)
                throw new HBPersistException("Error in SET command");

            if (var.equals("classpath")) {
                EnvVars.setClasspath(args.getValue());
                return;
            }

            throw new HBPersistException("Unknown variable: " + var);
        }

    }

    private static void createTable(final ClassSchema schema) throws IOException {

        final HTableDescriptor tableDesc = new HTableDescriptor(schema.getTableName());

        for (final HBFamily family : schema.getFamilies()) {
            final HColumnDescriptor columnDesc = new HColumnDescriptor(family.name());
            if (family.maxVersions() > 0)
                columnDesc.setMaxVersions(family.maxVersions());
            tableDesc.addFamily(columnDesc);
        }

        final HBaseAdmin admin = new HBaseAdmin(new HBaseConfiguration());

        admin.createTable(tableDesc);
    }

    public static void deleteAll(final String tablename) throws IOException {

        final HTable table = new HTable(new HBaseConfiguration(), tablename);

        final Scan scan = new Scan();
        final ResultScanner scanner = table.getScanner(scan);
        int cnt = 0;
        for (Result res : scanner) {
            final Delete delete = new Delete(res.getRow());
            table.delete(delete);
            cnt++;
        }
        System.out.println("Delete count: " + cnt);

    }

    public static void getFamilyInfo(final String tablename) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(new HBaseConfiguration());
        HTableDescriptor desc = admin.getTableDescriptor(tablename.getBytes());
        for (HColumnDescriptor col : desc.getFamilies()) {
            System.out.println("Family: " + col.getNameAsString());
            Map value = col.getValues();
            int r = 5;
        }

    }

}

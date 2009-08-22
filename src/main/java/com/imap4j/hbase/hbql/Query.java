package com.imap4j.hbase.hbql;

import com.google.common.collect.Lists;
import com.imap4j.hbase.antlr.QueryArgs;
import com.imap4j.hbase.antlr.config.HBqlRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scanner;
import org.apache.hadoop.hbase.io.RowResult;

import java.io.IOException;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 20, 2009
 * Time: 9:26:38 PM
 */
public class Query<T extends Persistable> {

    final String query;
    final QueryListener<T> listener;

    public Query(final String query, final QueryListener<T> listener) {
        this.query = query;
        this.listener = listener;
    }

    public void execute() throws IOException, PersistException {

        final QueryArgs qa = (QueryArgs)HBqlRule.QUERY.parse(this.query);

        final ClassSchema classSchema = ClassSchema.getClassSchema(qa.getTableName());

        final HTable table = new HTable(new HBaseConfiguration(), classSchema.getTableName());

        final List<String> colList = Lists.newArrayList();

        Object obj = null;
        try {
            obj = classSchema.getClazz().newInstance();

            for (final String attribName : qa.getColumnList()) {

                final FieldAttrib attrib = classSchema.getFieldAttribMapByField().get(attribName);

                colList.add(attrib.getFamily() + ":" + attrib.getColumn());

                //attrib.getField().set(obj, null);
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        Scanner scanner = table.getScanner(colList.toArray(new String[colList.size()]));

        int tot = 0;
        for (RowResult res : scanner) {
            String key = new String(res.getRow());
            System.out.println("Key: " + key);

            // table.deleteAll(key);
            tot++;
            //System.out.println(res);
            //for (byte[] b : res.keySet())
            //    System.out.println(new String(b));
        }
        System.out.println("Count: " + tot);

    }
}

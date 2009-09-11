package com.imap4j.hbase.hbase;

import com.imap4j.hbase.antlr.args.QueryArgs;
import com.imap4j.hbase.antlr.config.HBqlRule;
import com.imap4j.hbase.hbql.expr.ExprTree;
import com.imap4j.hbase.hbql.expr.ExprVariable;
import com.imap4j.hbase.hbql.schema.AnnotationSchema;
import com.imap4j.hbase.hbql.schema.ExprSchema;
import com.imap4j.hbase.hbql.schema.HUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import java.io.IOException;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 20, 2009
 * Time: 9:26:38 PM
 */
public class HQuery<T extends HPersistable> {

    final String query;
    final HQueryListener<T> listener;

    private HQuery(final String query, final HQueryListener<T> listener) {
        this.query = query;
        this.listener = listener;
    }

    public static <T extends HPersistable> HQuery<T> newHQuery(final String query, final HQueryListener<T> listener) {
        return new HQuery<T>(query, listener);
    }

    public String getQuery() {
        return this.query;
    }

    public HQueryListener<T> getListener() {
        return this.listener;
    }

    public void execute() throws IOException, HPersistException {

        final QueryArgs args = (QueryArgs)HBqlRule.SELECT.parse(this.getQuery(), (ExprSchema)null);
        final AnnotationSchema schema = AnnotationSchema.getAnnotationSchema(args.getTableName());
        final List<String> fieldList = (args.getColumnList() == null) ? schema.getFieldList() : args.getColumnList();

        final ExprTree clientFilter = args.getWhereExpr().getClientFilterArgs();
        if (clientFilter != null) {
            clientFilter.setSchema(schema);
            clientFilter.optimize();
        }

        // Check if all the variables referenced in the where clause are present in the fieldList.
        final List<ExprVariable> vars = clientFilter.getExprVariables();
        for (final ExprVariable var : vars) {
            if (!fieldList.contains(var.getName()))
                throw new HPersistException("Variable " + var.getName() + " used in client filter but it is not "
                                            + "not in the select list");
        }

        final List<Scan> scanList = HUtil.getScanList(schema,
                                                      fieldList,
                                                      args.getWhereExpr().getKeyRangeArgs(),
                                                      args.getWhereExpr().getVersionArgs(),
                                                      args.getWhereExpr().getServerFilterArgs());

        final HTable table = new HTable(new HBaseConfiguration(), schema.getTableName());

        for (final Scan scan : scanList) {
            ResultScanner resultScanner = null;
            try {
                resultScanner = table.getScanner(scan);
                for (final Result result : resultScanner) {
                    final HPersistable recordObj = HUtil.ser.getHPersistable(schema, scan, result);
                    if (clientFilter == null || clientFilter.evaluate(recordObj))
                        this.getListener().onEachRow((T)recordObj);
                }
            }
            finally {
                if (resultScanner != null)
                    resultScanner.close();
            }
        }
    }
}

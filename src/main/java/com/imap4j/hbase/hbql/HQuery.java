package com.imap4j.hbase.hbql;

import com.imap4j.hbase.antlr.args.QueryArgs;
import com.imap4j.hbase.antlr.config.HBqlRule;
import com.imap4j.hbase.hbql.expr.EvalContext;
import com.imap4j.hbase.hbql.expr.predicate.ExprEvalTree;
import com.imap4j.hbase.hbql.io.Serialization;
import com.imap4j.hbase.hbql.schema.ClassSchema;
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
    final HQueryListener<T> queryListener;

    public HQuery(final String query, final HQueryListener<T> queryListener) {
        this.query = query;
        this.queryListener = queryListener;
    }

    public String getQuery() {
        return this.query;
    }

    public HQueryListener<T> getQueryListener() {
        return this.queryListener;
    }

    public void execute() throws IOException, HPersistException {

        final QueryArgs args = (QueryArgs)HBqlRule.SELECT.parse(this.getQuery(), (ClassSchema)null);
        final ClassSchema classSchema = ClassSchema.getClassSchema(args.getTableName());
        final List<String> fieldList = (args.getColumnList() == null) ? classSchema.getFieldList() : args.getColumnList();
        final String tableName = classSchema.getTableName();
        final HTable table = new HTable(new HBaseConfiguration(), tableName);
        final Serialization ser = HSer.getSer();
        final ExprEvalTree clientFilter = args.getWhereExpr().getClientFilterArgs();
        final List<Scan> scanList = HUtil.getScanList(classSchema, fieldList, args.getWhereExpr());

        for (final Scan scan : scanList) {
            ResultScanner resultScanner = null;
            try {
                resultScanner = table.getScanner(scan);
                for (final Result result : resultScanner) {
                    final HPersistable recordObj = ser.getHPersistable(classSchema, scan, result);
                    if (clientFilter == null || clientFilter.evaluate(new EvalContext(classSchema, recordObj)))
                        this.getQueryListener().onEachRow((T)recordObj);
                }
            }
            finally {
                if (resultScanner != null)
                    resultScanner.close();
            }
        }
    }

}

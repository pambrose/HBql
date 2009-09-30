package org.apache.hadoop.hbase.hbql.query.antlr.cmds;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.HBqlFilter;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HConnection;
import org.apache.hadoop.hbase.hbql.client.HOutput;
import org.apache.hadoop.hbase.hbql.query.antlr.args.WhereArgs;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.query.schema.HBaseSchema;
import org.apache.hadoop.hbase.hbql.query.schema.VariableAttrib;

import java.io.IOException;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 20, 2009
 * Time: 11:43:49 PM
 */
public class DeleteCmd extends TableCmd implements ConnectionCmd {

    private final WhereArgs whereExpr;

    public DeleteCmd(final String tableName, final WhereArgs whereExpr) {
        super(tableName);
        this.whereExpr = whereExpr;
    }

    public WhereArgs getWhereExpr() {
        if (whereExpr == null)
            return new WhereArgs();
        else
            return this.whereExpr;
    }

    @Override
    public HOutput execute(final HConnection conn) throws HBqlException, IOException {

        final WhereArgs where = this.getWhereExpr();

        // TODO Need to grab schema from DeleteArgs (like QueryArgs in Select)
        final HBaseSchema schema = HBaseSchema.findSchema(this.getTableName());

        final List<VariableAttrib> attribList = schema.getAllVariableAttrib();
        final HTable table = conn.getHTable(schema.getTableName());
        final ExprTree clientFilter = where.getClientExprTree();
        clientFilter.setSchema(schema);
        int cnt = 0;

        final HBqlFilter serverFilter = schema.getHBqlFilter(where.getServerExprTree(),
                                                             attribList,
                                                             where.getScanLimit());

        final List<Scan> scanList = schema.getScanList(attribList,
                                                       where.getKeyRangeArgs(),
                                                       where.getTimeRangeArgs(),
                                                       where.getVersionArgs(),
                                                       serverFilter);

        for (final Scan scan : scanList) {
            final ResultScanner resultsScanner = table.getScanner(scan);
            for (final Result result : resultsScanner) {

                final Object recordObj = schema.newObject(attribList, scan.getMaxVersions(), result);

                if (clientFilter == null || clientFilter.evaluate(recordObj)) {
                    final Delete delete = new Delete(result.getRow());
                    table.delete(delete);
                    cnt++;
                }
            }
        }

        final HOutput retval = new HOutput();
        retval.out.println("Delete count: " + cnt);
        retval.out.flush();

        return retval;

    }

}

package org.apache.hadoop.hbase.hbql.query.cmds;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HConnection;
import org.apache.hadoop.hbase.hbql.client.HOutput;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.query.schema.ColumnAttrib;
import org.apache.hadoop.hbase.hbql.query.schema.HBaseSchema;
import org.apache.hadoop.hbase.hbql.query.stmt.args.WhereArgs;
import org.apache.hadoop.hbase.hbql.query.stmt.select.RowRequest;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 20, 2009
 * Time: 11:43:49 PM
 */
public class DeleteCmd extends TableCmd implements ConnectionCmd {

    private final WhereArgs whereArgs;

    public DeleteCmd(final String tableName, final WhereArgs whereArgs) {
        super(tableName);
        if (whereArgs == null)
            this.whereArgs = new WhereArgs();
        else
            this.whereArgs = whereArgs;
    }

    public WhereArgs getWhereArgs() {
        return this.whereArgs;
    }

    @Override
    public HOutput execute(final HConnection conn) throws HBqlException, IOException {

        final HBaseSchema schema = HBaseSchema.findSchema(this.getTableName());

        final WhereArgs where = this.getWhereArgs();
        where.setSchema(schema);

        final Set<ColumnAttrib> allWhereAttribs = this.getWhereArgs().getAllColumnsUsedInExprs();
        final HTable table = conn.getHTable(schema.getTableName());

        final List<RowRequest> rowRequestList = where.getRowRequestList(allWhereAttribs);

        int cnt = 0;

        for (final RowRequest rowRequest : rowRequestList)
            cnt += this.delete(table, where, rowRequest);

        final HOutput retval = new HOutput();
        retval.out.println("Delete count: " + cnt);
        retval.out.flush();

        return retval;
    }

    private int delete(final HTable table,
                       final WhereArgs where,
                       final RowRequest rowRequest) throws IOException, HBqlException {

        final ExprTree clientExprTree = where.getClientExprTree();

        int cnt = 0;
        for (final Result result : rowRequest.getResultScanner(table)) {
            if (clientExprTree == null || clientExprTree.evaluate(result)) {
                table.delete(new Delete(result.getRow()));
                cnt++;
            }
        }
        if (cnt > 0)
            table.flushCommits();
        return cnt;
    }
}

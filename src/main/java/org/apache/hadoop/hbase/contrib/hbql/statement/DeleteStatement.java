package org.apache.hadoop.hbase.contrib.hbql.statement;

import org.apache.expreval.client.HBqlException;
import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.expreval.expr.ExpressionTree;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.contrib.hbql.client.HOutput;
import org.apache.hadoop.hbase.contrib.hbql.impl.ConnectionImpl;
import org.apache.hadoop.hbase.contrib.hbql.schema.ColumnAttrib;
import org.apache.hadoop.hbase.contrib.hbql.statement.args.WithArgs;
import org.apache.hadoop.hbase.contrib.hbql.statement.select.RowRequest;

import java.io.IOException;
import java.util.List;
import java.util.Set;

public class DeleteStatement extends SchemaStatement implements ConnectionStatement {

    private final WithArgs withArgs;

    public DeleteStatement(final String schemaName, final WithArgs withArgs) {
        super(schemaName);
        if (withArgs == null)
            this.withArgs = new WithArgs();
        else
            this.withArgs = withArgs;
    }

    public WithArgs getWithArgs() {
        return this.withArgs;
    }

    public HOutput execute(final ConnectionImpl conn) throws HBqlException, IOException {

        this.getWithArgs().setSchema(this.getSchema());

        final Set<ColumnAttrib> allWhereAttribs = this.getWithArgs().getAllColumnsUsedInExprs();
        final HTable table = conn.getHTable(this.getSchema().getTableName());

        final List<RowRequest> rowRequestList = this.getWithArgs().getRowRequestList(allWhereAttribs);

        int cnt = 0;

        for (final RowRequest rowRequest : rowRequestList)
            cnt += this.delete(table, this.getWithArgs(), rowRequest);

        return new HOutput("Delete count: " + cnt);
    }

    private int delete(final HTable table,
                       final WithArgs with,
                       final RowRequest rowRequest) throws IOException, HBqlException {

        final ExpressionTree clientExpressionTree = with.getClientExpressionTree();

        int cnt = 0;
        for (final Result result : rowRequest.getResultScanner(table)) {
            try {
                if (clientExpressionTree == null || clientExpressionTree.evaluate(result)) {
                    table.delete(new Delete(result.getRow()));
                    cnt++;
                }
            }
            catch (ResultMissingColumnException e) {
                // Just skip and do nothing
            }
        }
        if (cnt > 0)
            table.flushCommits();
        return cnt;
    }
}

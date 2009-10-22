package org.apache.hadoop.hbase.hbql.query.cmds.record;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HOutput;
import org.apache.hadoop.hbase.hbql.client.ResultMissingColumnException;
import org.apache.hadoop.hbase.hbql.query.cmds.ConnectionCmd;
import org.apache.hadoop.hbase.hbql.query.cmds.SchemaCmd;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.query.impl.hbase.ConnectionImpl;
import org.apache.hadoop.hbase.hbql.query.schema.ColumnAttrib;
import org.apache.hadoop.hbase.hbql.query.schema.HBaseSchema;
import org.apache.hadoop.hbase.hbql.query.stmt.args.WhereArgs;
import org.apache.hadoop.hbase.hbql.query.stmt.select.RowRequest;

import java.io.IOException;
import java.util.List;
import java.util.Set;

public class DeleteRecordsCmd extends SchemaCmd implements ConnectionCmd {

    private final WhereArgs whereArgs;

    public DeleteRecordsCmd(final String schemaName, final WhereArgs whereArgs) {
        super(schemaName);
        if (whereArgs == null)
            this.whereArgs = new WhereArgs();
        else
            this.whereArgs = whereArgs;
    }

    public WhereArgs getWhereArgs() {
        return this.whereArgs;
    }

    public HOutput execute(final ConnectionImpl conn) throws HBqlException, IOException {

        final HBaseSchema schema = HBaseSchema.findSchema(this.getSchemaName());
        this.getWhereArgs().setSchema(schema);

        final Set<ColumnAttrib> allWhereAttribs = this.getWhereArgs().getAllColumnsUsedInExprs();
        final HTable table = conn.getHTable(schema.getTableName());

        final List<RowRequest> rowRequestList = this.getWhereArgs().getRowRequestList(allWhereAttribs);

        int cnt = 0;

        for (final RowRequest rowRequest : rowRequestList)
            cnt += this.delete(table, this.getWhereArgs(), rowRequest);

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
            try {
                if (clientExprTree == null || clientExprTree.evaluate(result)) {
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

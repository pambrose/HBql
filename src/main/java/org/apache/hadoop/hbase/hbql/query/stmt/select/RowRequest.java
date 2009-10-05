package org.apache.hadoop.hbase.hbql.query.stmt.select;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.schema.ColumnAttrib;
import org.apache.hadoop.hbase.hbql.query.stmt.args.KeyRangeArgs;
import org.apache.hadoop.hbase.hbql.query.stmt.args.WhereArgs;
import org.apache.hadoop.hbase.hbql.query.util.Lists;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Oct 5, 2009
 * Time: 1:14:01 PM
 */
public class RowRequest {

    final Get getValue;
    final Scan scanValue;

    public RowRequest(final WhereArgs whereArgs,
                      final Collection<ColumnAttrib> columnAttribSet,
                      final KeyRangeArgs.Range range) throws HBqlException, IOException {

        if (range.isSinlgeRow()) {
            this.getValue = range.getGet(whereArgs, columnAttribSet);
            this.scanValue = null;
        }
        else {
            this.getValue = null;
            this.scanValue = range.getScan(whereArgs, columnAttribSet);
        }

    }

    public boolean isAScan() {
        return this.getScanValue() != null;
    }

    public Get getGetValue() {
        return this.getValue;
    }

    public Scan getScanValue() {
        return this.scanValue;
    }

    public ResultScanner getResultScanner(final HTable table) throws IOException {
        if (this.isAScan()) {
            return table.getScanner(this.getScanValue());
        }
        else {
            final List<Result> resultList = Lists.newArrayList();
            final Result result = table.get(this.getGetValue());
            if (result != null)
                resultList.add(result);

            return new ResultScanner() {
                @Override
                public Result next() throws IOException {
                    return null;
                }

                @Override
                public Result[] next(final int nbRows) throws IOException {
                    return null;
                }

                @Override
                public Iterator<Result> iterator() {
                    return resultList.iterator();
                }

                @Override
                public void close() {

                }

            };

        }
    }
}

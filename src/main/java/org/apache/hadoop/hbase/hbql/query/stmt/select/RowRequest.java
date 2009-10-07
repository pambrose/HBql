package org.apache.hadoop.hbase.hbql.query.stmt.select;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.hbql.query.util.Lists;

import java.io.IOException;
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

    public RowRequest(final Get getValue, final Scan scanValue) {
        this.getValue = getValue;
        this.scanValue = scanValue;
    }

    private boolean isAScan() {
        return this.getScanValue() != null;
    }

    private Get getGetValue() {
        return this.getValue;
    }

    private Scan getScanValue() {
        return this.scanValue;
    }

    public int getMaxVersions() {
        if (this.isAScan())
            return this.getScanValue().getMaxVersions();
        else
            return this.getGetValue().getMaxVersions();
    }


    public ResultScanner getResultScanner(final HTable table) throws IOException {

        // If we are dealing with a Get, then we need to fake a ResultScanner with the Get result
        if (this.isAScan()) {
            return table.getScanner(this.getScanValue());
        }
        else {
            final Result result = table.get(this.getGetValue());
            final List<Result> resultList = Lists.newArrayList();
            if (result != null && !result.isEmpty())
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

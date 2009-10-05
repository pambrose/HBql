package org.apache.hadoop.hbase.hbql.query.stmt.select;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.stmt.args.KeyRangeArgs;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Oct 5, 2009
 * Time: 1:14:01 PM
 */
public class RowRequest {

    final Get getValue;
    final Scan scanValue;

    public RowRequest(final KeyRangeArgs.Range range) throws HBqlException {

        if (range.isSinlgeRow()) {
            this.getValue = range.getGet();
            this.scanValue = null;
        }
        else {
            this.getValue = null;
            this.scanValue = range.getScan();
        }

    }
}

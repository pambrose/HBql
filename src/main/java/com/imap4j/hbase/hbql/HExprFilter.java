package com.imap4j.hbase.hbql;

import com.imap4j.hbase.hbql.expr.predicate.WhereExpr;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 29, 2009
 * Time: 11:51:05 PM
 */
public class HExprFilter implements Filter {

    WhereExpr filterExpr;

    public HExprFilter(final WhereExpr filterExpr) {
        this.filterExpr = filterExpr;
    }

    public WhereExpr getFilterExpr() {
        return filterExpr;
    }

    @Override
    public void reset() {

    }

    @Override
    public boolean filterRowKey(final byte[] buffer, final int offset, final int length) {
        return false;
    }

    @Override
    public boolean filterAllRemaining() {
        return false;
    }

    @Override
    public ReturnCode filterKeyValue(final KeyValue v) {
        return null;
    }

    @Override
    public boolean filterRow() {
        return false;
    }

    @Override
    public void write(final DataOutput out) throws IOException {
        final byte[] b = HUtil.getObjectAsBytes(this.getFilterExpr());
        Bytes.writeByteArray(out, b);
    }

    @Override
    public void readFields(final DataInput in) throws IOException {
        byte[] b = Bytes.readByteArray(in);

        try {
            this.filterExpr = (WhereExpr)HUtil.getObjectFromBytes(b);
        }
        catch (HPersistException e) {
            e.printStackTrace();
            throw new IOException(e.getCause());
        }
    }
}

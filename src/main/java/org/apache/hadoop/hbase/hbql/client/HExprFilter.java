package org.apache.hadoop.hbase.hbql.client;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;

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

    ExprTree filterExpr;

    public HExprFilter(final ExprTree filterExpr) {
        this.filterExpr = filterExpr;
    }

    public ExprTree getFilterExpr() {
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
        System.out.println("Called by filterKeyValue() - " + new String(v.getColumn()) + " - " + v.getKeyString());
        return ReturnCode.INCLUDE;
    }

    @Override
    public boolean filterRow() {
        return false;
    }

    @Override
    public void write(final DataOutput out) throws IOException {
        //final byte[] b = JavaSerialization.getObjectAsBytes(this.getFilterExpr());
        // Bytes.writeByteArray(out, b);

    }

    @Override
    public void readFields(final DataInput in) throws IOException {

        /*
        byte[] b = Bytes.readByteArray(in);

        try {
           // this.filterExpr = (ExprEvalTree)JavaSerialization.getObjectFromBytes(b);
        }
        catch (HPersistException e) {
            e.printStackTrace();
            throw new IOException("HPersist problem: " + e.getCause());
        }
        */
    }
}

package org.apache.hadoop.hbase.contrib.hbql.statement.args;

import org.apache.expreval.expr.node.GenericValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.contrib.hbql.client.HBqlException;

import java.io.IOException;

public class TimestampArgs extends SelectArgs {

    final boolean singleValue;

    public TimestampArgs(final GenericValue arg0, final GenericValue arg1) {
        super(SelectArgs.Type.TIMESTAMPRANGE, arg0, arg1);
        this.singleValue = arg0 == arg1;
    }

    private long getLower() throws HBqlException {
        return (Long)this.evaluateConstant(0, false, null);
    }

    private long getUpper() throws HBqlException {
        return (Long)this.evaluateConstant(1, false, null);
    }

    private boolean isSingleValue() {
        return this.singleValue;
    }

    public String asString() {
        if (this.isSingleValue())
            return "TIMESTAMP " + this.getGenericValue(0).asString();
        else
            return "TIMESTAMP RANGE " + this.getGenericValue(0).asString() + " TO "
                   + this.getGenericValue(1).asString();
    }

    public void setTimeStamp(final Get get) throws HBqlException, IOException {
        if (this.isSingleValue())
            get.setTimeStamp(this.getLower());
        else
            get.setTimeRange(this.getLower(), this.getUpper());
    }

    public void setTimeStamp(final Scan scan) throws HBqlException, IOException {
        if (this.isSingleValue())
            scan.setTimeStamp(this.getLower());
        else
            scan.setTimeRange(this.getLower(), this.getUpper());
    }
}
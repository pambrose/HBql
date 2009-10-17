package org.apache.hadoop.hbase.hbql.query.stmt.args;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;

import java.io.IOException;

public class TimeRangeArgs extends SelectArgs {

    public TimeRangeArgs(final GenericValue arg0, final GenericValue arg1) {
        super(SelectArgs.Type.TIMERANGE, arg0, arg1);
    }

    private long getLower() throws HBqlException {
        return (Long)this.noColumnEvaluate(0, false, false, null);
    }

    private long getUpper() throws HBqlException {
        return (Long)this.noColumnEvaluate(1, false, false, null);
    }

    public String asString() {
        return "TIME RANGE " + this.getGenericValue(0).asString() + " TO " + this.getGenericValue(1);
    }

    public void setTimeStamp(final Get get) throws HBqlException, IOException {
        if (this.getLower() == this.getUpper())
            get.setTimeStamp(this.getLower());
        else
            get.setTimeRange(this.getLower(), this.getUpper());
    }

    public void setTimeStamp(final Scan scan) throws HBqlException, IOException {
        if (this.getLower() == this.getUpper())
            scan.setTimeStamp(this.getLower());
        else
            scan.setTimeRange(this.getLower(), this.getUpper());
    }
}
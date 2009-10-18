package org.apache.hadoop.hbase.hbql.query.stmt.args;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;

import java.io.IOException;

public class VersionArgs extends SelectArgs {

    public VersionArgs(final GenericValue val) {
        super(SelectArgs.Type.VERSION, val);
    }

    private int getValue() throws HBqlException {
        return ((Number)this.evaluateWithoutColumns(0, false, null)).intValue();
    }

    public String asString() {
        return "VERSIONS " + this.getGenericValue(0).asString();
    }

    public void setMaxVersions(final Get get) throws HBqlException, IOException {
        final int max = this.getValue();
        if (max == Integer.MAX_VALUE)
            get.setMaxVersions();
        else
            get.setMaxVersions(max);
    }

    public void setMaxVersions(final Scan scan) throws HBqlException {
        final int max = this.getValue();
        if (max == Integer.MAX_VALUE)
            scan.setMaxVersions();
        else
            scan.setMaxVersions(max);
    }
}
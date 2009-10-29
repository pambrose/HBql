package org.apache.hadoop.hbase.contrib.hbql.impl;

import org.apache.hadoop.hbase.client.Put;

import java.io.IOException;

public class InsertAction implements BatchAction {

    private final Put actionValue;

    public InsertAction(final Put actionValue) {
        this.actionValue = actionValue;
    }

    private Put getActionValue() {
        return this.actionValue;
    }

    public void apply(final org.apache.hadoop.hbase.client.HTable table) throws IOException {
        table.put(this.getActionValue());
    }

    public String toString() {
        return "INSERT";
    }
}

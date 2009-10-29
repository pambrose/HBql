package org.apache.hadoop.hbase.contrib.hbql.impl;

import org.apache.hadoop.hbase.client.Delete;

import java.io.IOException;

public class DeleteAction implements BatchAction {

    private final Delete actionValue;

    public DeleteAction(final Delete actionValue) {
        this.actionValue = actionValue;
    }

    private Delete getActionValue() {
        return this.actionValue;
    }

    public void apply(final org.apache.hadoop.hbase.client.HTable table) throws IOException {
        table.delete(this.getActionValue());
    }

    public String toString() {
        return "DELETE";
    }
}
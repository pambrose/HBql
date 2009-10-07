package org.apache.hadoop.hbase.hbql.client;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;

import java.io.IOException;

public class HBatchAction {

    private enum Type {
        INSERT, DELETE
    }

    private final Type type;
    private final Object actionValue;

    HBatchAction(final Type type, final Object actionValue) {
        this.type = type;
        this.actionValue = actionValue;
    }

    static HBatchAction newInsert(final Put put) {
        return new HBatchAction(Type.INSERT, put);
    }

    static HBatchAction newDelete(final Delete delete) {
        return new HBatchAction(Type.DELETE, delete);
    }

    private boolean isInsert() {
        return this.type == Type.INSERT;
    }

    private boolean isDelete() {
        return this.type == Type.DELETE;
    }

    private Put getPutValue() {
        return (Put)this.actionValue;
    }

    private Delete getDeleteValue() {
        return (Delete)this.actionValue;
    }

    public void apply(final org.apache.hadoop.hbase.client.HTable table) throws IOException {

        if (this.isInsert())
            table.put(this.getPutValue());

        if (this.isDelete())
            table.delete(this.getDeleteValue());
    }

    public String toString() {
        return this.type.name();
    }
}

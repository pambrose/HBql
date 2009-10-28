package org.apache.expreval.statement;

public abstract class TableStatement {

    private final String tableName;

    protected TableStatement(final String tableName) {
        this.tableName = tableName;
    }

    protected String getTableName() {
        return tableName;
    }
}

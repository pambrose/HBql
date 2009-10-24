package org.apache.hadoop.hbase.hbql.stmt.schema;

import org.apache.hadoop.hbase.hbql.stmt.SchemaStatement;
import org.apache.hadoop.hbase.hbql.stmt.expr.node.GenericValue;

import java.util.List;

public class InsertStatement extends SchemaStatement {

    private final List<GenericValue> columnList;
    private final List<GenericValue> valueList;

    public InsertStatement(final String schemaName,
                           final List<GenericValue> columnList,
                           final List<GenericValue> valueList) {
        super(schemaName);
        this.columnList = columnList;
        this.valueList = valueList;
    }
}
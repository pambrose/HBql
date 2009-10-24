package org.apache.hadoop.hbase.hbql.stmt.schema;

import org.apache.hadoop.hbase.hbql.client.HBatch;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HOutput;
import org.apache.hadoop.hbase.hbql.client.HRecord;
import org.apache.hadoop.hbase.hbql.client.PreparedStatement;
import org.apache.hadoop.hbase.hbql.client.SchemaManager;
import org.apache.hadoop.hbase.hbql.query.impl.hbase.ConnectionImpl;
import org.apache.hadoop.hbase.hbql.stmt.SchemaStatement;
import org.apache.hadoop.hbase.hbql.stmt.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.stmt.select.ExprElement;
import org.apache.hadoop.hbase.hbql.stmt.util.Lists;

import java.io.IOException;
import java.util.List;

public class InsertStatement extends SchemaStatement implements PreparedStatement {

    private final List<ExprElement> columnList = Lists.newArrayList();
    private final List<ExprElement> valueList = Lists.newArrayList();

    private ConnectionImpl connection = null;
    private HRecord record = null;

    public InsertStatement(final String schemaName,
                           final List<GenericValue> columnList,
                           final List<GenericValue> valueList) {
        super(schemaName);

        for (final GenericValue val : columnList)
            this.columnList.add(ExprElement.newExprElement(val, null));

        for (final GenericValue val : valueList)
            this.valueList.add(ExprElement.newExprElement(val, null));
    }

    public void setConnection(final ConnectionImpl connection) throws HBqlException {
        this.connection = connection;
        this.record = SchemaManager.newHRecord(this.getSchemaName());
    }

    public void validate() throws HBqlException {

        for (final ExprElement val : this.columnList)
            val.validate(this.getSchema(), this.getConnection());

        for (final ExprElement val : this.valueList)
            val.validate(this.getSchema(), this.getConnection());
    }

    public int setParameter(final String name, final Object val) throws HBqlException {

        int cnt = 0;

        for (final ExprElement expr : this.valueList)
            cnt += expr.setParameter(name, val);

        return cnt;
    }

    private HRecord getRecord() {
        return this.record;
    }

    private ConnectionImpl getConnection() {
        return this.connection;
    }

    public HOutput execute() throws HBqlException, IOException {

        HBatch batch = new HBatch();
        batch.insert(this.getRecord());

        this.getConnection().apply(batch);

        this.getRecord().reset();

        return null;
    }
}
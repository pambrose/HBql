package org.apache.hadoop.hbase.hbql.stmt.expr.var;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.ResultMissingColumnException;
import org.apache.hadoop.hbase.hbql.query.schema.ColumnAttrib;
import org.apache.hadoop.hbase.hbql.stmt.expr.ExprContext;
import org.apache.hadoop.hbase.hbql.stmt.expr.node.GenericValue;

public class DelegateColumn extends GenericColumn<GenericValue> {

    private GenericColumn typedColumn = null;
    private final String variableName;

    public DelegateColumn(final String variableName) {
        super(null);
        this.variableName = variableName;
    }

    private GenericColumn getTypedColumn() {
        return this.typedColumn;
    }

    private void setTypedColumn(final GenericColumn typedColumn) {
        this.typedColumn = typedColumn;
    }

    public String getVariableName() {
        return this.variableName;
    }

    public Object getValue(final Object object) throws HBqlException, ResultMissingColumnException {
        return this.getTypedColumn().getValue(object);
    }

    public Class<? extends GenericValue> validateTypes(final GenericValue parentExpr,
                                                       final boolean allowsCollections) throws HBqlException {
        return this.getTypedColumn().validateTypes(parentExpr, allowsCollections);
    }

    public void setExprContext(final ExprContext context) throws HBqlException {

        // See if referenced var is in schema
        final ColumnAttrib attrib = context.getSchema().getAttribByVariableName(this.getVariableName());

        if (attrib == null)
            throw new HBqlException("Invalid variable: " + this.getVariableName());

        switch (attrib.getFieldType()) {

            case KeyType:
            case StringType:
                this.setTypedColumn(new StringColumn(attrib));
                break;

            case BooleanType:
                this.setTypedColumn(new BooleanColumn(attrib));
                break;

            case ByteType:
                this.setTypedColumn(new ByteColumn(attrib));
                break;

            case CharType:
                this.setTypedColumn(new CharColumn(attrib));
                break;

            case ShortType:
                this.setTypedColumn(new ShortColumn(attrib));
                break;

            case IntegerType:
                this.setTypedColumn(new IntegerColumn(attrib));
                break;

            case LongType:
                this.setTypedColumn(new LongColumn(attrib));
                break;

            case FloatType:
                this.setTypedColumn(new FloatColumn(attrib));
                break;

            case DoubleType:
                this.setTypedColumn(new DoubleColumn(attrib));
                break;

            case DateType:
                this.setTypedColumn(new DateColumn(attrib));
                break;

            case ObjectType:
                this.setTypedColumn(new ObjectColumn(attrib));
                break;

            default:
                throw new HBqlException("Invalid type: " + attrib.getFieldType().name());
        }

        this.getTypedColumn().setExprContext(context);
    }
}

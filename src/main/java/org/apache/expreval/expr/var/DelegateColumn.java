package org.apache.expreval.expr.var;

import org.apache.expreval.expr.ExpressionContext;
import org.apache.expreval.expr.node.GenericValue;
import org.apache.expreval.schema.ColumnAttrib;
import org.apache.hadoop.hbase.contrib.hbql.client.HBqlException;
import org.apache.hadoop.hbase.contrib.hbql.client.ResultMissingColumnException;

public class DelegateColumn extends GenericColumn<GenericValue> {

    private GenericColumn typedColumn = null;
    private final String variableName;

    public DelegateColumn(final String variableName) {
        super(null);
        this.variableName = variableName;
    }

    public GenericColumn getTypedColumn() {
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

    public void setExprContext(final ExpressionContext context) throws HBqlException {

        if (context.getSchema() == null)
            throw new HBqlException("Internal error: null schema");

        // See if referenced var is in schema
        final ColumnAttrib attrib = context.getSchema().getAttribByVariableName(this.getVariableName());

        if (attrib == null)
            throw new HBqlException("Invalid variable: " + this.getVariableName());

        switch (attrib.getFieldType()) {

            case KeyType:
                this.setTypedColumn(new KeyColumn(attrib));
                break;

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

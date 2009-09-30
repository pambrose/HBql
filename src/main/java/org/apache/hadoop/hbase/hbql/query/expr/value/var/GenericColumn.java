package org.apache.hadoop.hbase.hbql.query.expr.value.var;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.TypeException;
import org.apache.hadoop.hbase.hbql.query.expr.ExprContext;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.schema.FieldType;
import org.apache.hadoop.hbase.hbql.query.schema.VariableAttrib;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 31, 2009
 * Time: 12:30:57 PM
 */
public abstract class GenericColumn<T extends GenericValue> implements GenericValue {

    private final FieldType fieldType;
    private final VariableAttrib variableAttrib;

    private ExprContext context = null;

    protected GenericColumn(final VariableAttrib attrib, final FieldType fieldType) {
        this.variableAttrib = attrib;
        this.fieldType = fieldType;
    }

    public VariableAttrib getVariableAttrib() {
        return this.variableAttrib;
    }

    public String getVariableName() {
        return this.getVariableAttrib().getVariableName();
    }

    public String getFamilyQualifiedName() {
        return this.getVariableAttrib().getFamilyQualifiedName();
    }

    private FieldType getFieldType() {
        return this.fieldType;
    }

    @Override
    public T getOptimizedValue() throws HBqlException {
        return (T)this;
    }

    public boolean isAConstant() throws HBqlException {
        return false;
    }

    @Override
    public void setContext(final ExprContext context) throws HBqlException {
        this.context = context;
        this.getContext().addVariable(this);
    }

    protected ExprContext getContext() {
        return this.context;
    }

    @Override
    public Class<? extends GenericValue> validateTypes(final GenericValue parentExpr,
                                                       final boolean allowsCollections) throws TypeException {
        return this.getFieldType().getExprType();
    }

    @Override
    public String asString() {
        return this.getVariableName();
    }
}

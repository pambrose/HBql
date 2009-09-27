package org.apache.hadoop.hbase.hbql.query.expr.value.var;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.TypeException;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;
import org.apache.hadoop.hbase.hbql.query.schema.FieldType;
import org.apache.hadoop.hbase.hbql.query.schema.VariableAttrib;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 31, 2009
 * Time: 12:30:57 PM
 */
public abstract class GenericVariable<T extends ValueExpr> implements ValueExpr {

    private final String attribName;
    private final FieldType fieldType;
    private final VariableAttrib variableAttrib;

    private ExprTree context = null;

    protected GenericVariable(final VariableAttrib attrib, final FieldType fieldType) {
        this.attribName = attrib.getVariableName();
        this.fieldType = fieldType;
        this.variableAttrib = attrib;
    }

    protected GenericVariable(final String attribName) {
        this.attribName = attribName;
        this.fieldType = null;
        this.variableAttrib = null;
    }

    public String getName() {
        return this.attribName;
    }

    public FieldType getFieldType() {
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
    public void setContext(final ExprTree context) {
        this.context = context;
        this.getContext().addVariable(this);
    }

    protected ExprTree getContext() {
        return this.context;
    }

    protected VariableAttrib getVariableAttrib() throws HBqlException {
        return this.variableAttrib;
    }

    @Override
    public Class<? extends ValueExpr> validateTypes(final ValueExpr parentExpr,
                                                    final boolean allowsCollections) throws TypeException {
        return this.getFieldType().getExprType();
    }

    @Override
    public String asString() {
        return this.getName();
    }
}

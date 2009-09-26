package org.apache.hadoop.hbase.hbql.query.expr.value.var;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.TypeException;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.query.expr.ExprVariable;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;
import org.apache.hadoop.hbase.hbql.query.schema.FieldType;
import org.apache.hadoop.hbase.hbql.query.schema.VariableAttrib;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 31, 2009
 * Time: 12:30:57 PM
 */
public abstract class GenericAttribRef<T extends ValueExpr> implements ValueExpr {

    private final ExprVariable exprVar;
    private final VariableAttrib variableAttrib;

    private ExprTree context = null;

    protected GenericAttribRef(final VariableAttrib attrib, final FieldType fieldType) {
        this.exprVar = new ExprVariable(attrib.getVariableName(), fieldType);
        this.variableAttrib = attrib;
    }

    protected GenericAttribRef(final String attribName) {
        this.exprVar = new ExprVariable(attribName, null);
        this.variableAttrib = null;
    }

    public ExprVariable getExprVar() {
        return this.exprVar;
    }

    public String getName() {
        return this.getExprVar().getName();
    }

    @Override
    public T getOptimizedValue() throws HBqlException {
        return (T)this;
    }

    public boolean isAConstant() {
        return false;
    }

    @Override
    public void setContext(final ExprTree context) {
        this.context = context;
        this.getContext().addAttribRef(this);
    }

    protected ExprTree getContext() {
        return this.context;
    }

    protected VariableAttrib getVariableAttrib() throws HBqlException {
        return this.variableAttrib;
    }

    @Override
    public Class<? extends ValueExpr> validateTypes(final ValueExpr parentExpr) throws TypeException {
        return this.getExprVar().getFieldType().getExprType();
    }

    @Override
    public String asString() {
        return this.getName();
    }
}

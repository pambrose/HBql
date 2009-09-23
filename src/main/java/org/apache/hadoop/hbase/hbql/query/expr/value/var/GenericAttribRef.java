package org.apache.hadoop.hbase.hbql.query.expr.value.var;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.query.expr.ExprVariable;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;
import org.apache.hadoop.hbase.hbql.query.schema.FieldType;
import org.apache.hadoop.hbase.hbql.query.schema.Schema;
import org.apache.hadoop.hbase.hbql.query.util.Lists;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 31, 2009
 * Time: 12:30:57 PM
 */
public abstract class GenericAttribRef<T extends ValueExpr> {

    private final ExprVariable exprVar;
    private ExprTree context = null;

    protected GenericAttribRef(final String attribName, final FieldType fieldType) {
        this.exprVar = new ExprVariable(attribName, fieldType);
    }

    public ExprVariable getExprVar() {
        return this.exprVar;
    }

    public List<ExprVariable> getExprVariables() {
        return Lists.newArrayList(this.getExprVar());
    }

    public T getOptimizedValue() throws HPersistException {
        return (T)this;
    }

    public boolean isAConstant() {
        return false;
    }

    public void setContext(final ExprTree context) {
        this.context = context;
    }

    protected ExprTree getContext() {
        return this.context;
    }

    protected Schema getSchema() {
        return this.getContext().getSchema();
    }

    public Class<? extends ValueExpr> validateType() throws HPersistException {
        return this.exprVar.getFieldType().getExprType();
    }

}

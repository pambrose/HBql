package org.apache.hadoop.hbase.hbql.query.expr.value.var;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.query.expr.ExprVariable;
import org.apache.hadoop.hbase.hbql.query.expr.node.ExprTreeNode;
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
public abstract class GenericAttribRef<T extends ValueExpr> implements ExprTreeNode {

    private final ExprVariable exprVar;
    private ExprTree context = null;

    protected GenericAttribRef(final String attribName, final FieldType fieldType) {
        this.exprVar = new ExprVariable(attribName, fieldType);
    }

    public ExprVariable getExprVar() {
        return this.exprVar;
    }

    @Override
    public List<ExprVariable> getExprVariables() {
        return Lists.newArrayList(this.getExprVar());
    }

    @Override
    public T getOptimizedValue(final Object object) throws HPersistException {
        return (T)this;
    }

    @Override
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
}

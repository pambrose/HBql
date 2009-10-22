package org.apache.hadoop.hbase.hbql.query.expr.value.function;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.ResultMissingColumnException;
import org.apache.hadoop.hbase.hbql.client.TypeException;
import org.apache.hadoop.hbase.hbql.query.expr.ExprContext;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.expr.value.stmt.DelegateStmt;

import java.util.List;

public class DelegateFunction extends DelegateStmt<Function> {

    private final String functionName;
    private ExprContext context = null;

    public DelegateFunction(final String functionName, final List<GenericValue> exprList) {
        super(null, exprList);
        this.functionName = functionName;
    }


    public Class<? extends GenericValue> validateTypes(final GenericValue parentExpr,
                                                       final boolean allowsCollections) throws TypeException {

        Function function = Function.Type.getFunction(this.getFunctionName(), this.getArgList());

        if (function == null)
            function = DateFunction.IntervalType.getFunction(this.getFunctionName(), this.getArgList());

        if (function == null)
            function = DateFunction.ConstantType.getFunction(this.getFunctionName());

        if (function == null)
            this.throwInvalidTypeException();

        this.setTypedExpr(function);

        // TODO This needs to be cleaned up and done in other delegates that create objects in validateTypes
        try {
            this.getTypedExpr().setExprContext(this.context);
        }
        catch (HBqlException e) {
            throw new TypeException(e.getMessage());
        }

        return this.getTypedExpr().validateTypes(parentExpr, false);
    }

    public void setExprContext(final ExprContext context) throws HBqlException {
        super.setExprContext(context);
        this.context = context;
    }

    public GenericValue getOptimizedValue() throws HBqlException {
        this.optimizeArgs();
        return !this.isAConstant() ? this : this.getTypedExpr().getOptimizedValue();
    }

    public Object getValue(final Object object) throws HBqlException, ResultMissingColumnException {
        return this.getTypedExpr().getValue(object);
    }

    public String getFunctionName() {
        return this.functionName;
    }
}

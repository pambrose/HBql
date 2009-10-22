package org.apache.hadoop.hbase.hbql.query.expr.value.function;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.InvalidFunctionException;
import org.apache.hadoop.hbase.hbql.client.ResultMissingColumnException;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.expr.value.stmt.DelegateStmt;

import java.util.List;

public class DelegateFunction extends DelegateStmt<Function> {

    private final String functionName;

    public DelegateFunction(final String functionName, final List<GenericValue> exprList) {
        super(null, exprList);
        this.functionName = functionName;
    }

    public Class<? extends GenericValue> validateTypes(final GenericValue parentExpr,
                                                       final boolean allowsCollections) throws HBqlException {

        Function function = Function.FunctionType.getFunction(this.getFunctionName(), this.getArgList());

        if (function == null)
            function = DateFunction.IntervalType.getFunction(this.getFunctionName(), this.getArgList());

        if (function == null)
            function = DateFunction.ConstantType.getFunction(this.getFunctionName());

        if (function == null)
            throw new InvalidFunctionException(this.getFunctionName() + " in " + parentExpr.asString());

        this.setTypedExpr(function);

        return this.getTypedExpr().validateTypes(parentExpr, false);
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

    public String asString() {
        return this.getFunctionName() + super.asString();
    }
}

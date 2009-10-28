package org.apache.expreval.expr.function;

import org.apache.expreval.client.HBqlException;
import org.apache.expreval.client.InvalidFunctionException;
import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.expreval.expr.DelegateStmt;
import org.apache.expreval.expr.node.GenericValue;

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

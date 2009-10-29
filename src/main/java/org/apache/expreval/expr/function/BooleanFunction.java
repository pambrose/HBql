package org.apache.expreval.expr.function;

import org.apache.expreval.client.HBqlException;
import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.expreval.client.TypeException;
import org.apache.expreval.expr.ExpressionContext;
import org.apache.expreval.expr.ExpressionTree;
import org.apache.expreval.expr.node.BooleanValue;
import org.apache.expreval.expr.node.GenericValue;
import org.apache.expreval.expr.var.DelegateColumn;
import org.apache.hadoop.hbase.contrib.hbql.parser.Parser;
import org.apache.hadoop.hbase.contrib.hbql.schema.Schema;

import java.util.List;

public class BooleanFunction extends Function implements BooleanValue {

    private Schema schema = null;

    public BooleanFunction(final FunctionType functionType, final List<GenericValue> exprs) {
        super(functionType, exprs);
    }

    public Class<? extends GenericValue> validateTypes(final GenericValue parentExpr,
                                                       final boolean allowsCollections) throws HBqlException {

        switch (this.getFunctionType()) {

            case DEFINEDINROW: {
                if (!(this.getArg(0) instanceof DelegateColumn))
                    throw new TypeException("Argument should be a column reference in: " + this.asString());
            }
        }

        return BooleanValue.class;
        //return super.validateTypes(parentExpr, allowsCollections);
    }

    public void setExprContext(final ExpressionContext context) throws HBqlException {
        super.setExprContext(context);
        this.schema = context.getSchema();
    }

    private Schema getSchema() {
        return this.schema;
    }

    public Boolean getValue(final Object object) throws HBqlException, ResultMissingColumnException {

        switch (this.getFunctionType()) {

            case RANDOMBOOLEAN: {
                return Function.randomVal.nextBoolean();
            }

            case DEFINEDINROW: {
                try {
                    this.getArg(0).getValue(object);
                    return true;
                }
                catch (ResultMissingColumnException e) {
                    return false;
                }
            }

            case EVAL: {
                final String exprStr = (String)this.getArg(0).getValue(object);
                final ExpressionTree expressionTree = Parser.parseWhereExpression(exprStr, this.getSchema());
                return expressionTree.evaluate(object);
            }

            default:
                throw new HBqlException("Invalid function: " + this.getFunctionType());
        }
    }
}
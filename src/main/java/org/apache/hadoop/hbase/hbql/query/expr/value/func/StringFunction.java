package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.query.expr.node.StringValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 29, 2009
 * Time: 5:28:17 PM
 */
public class StringFunction extends GenericFunction implements StringValue {

    public StringFunction(final FunctionType functionType, final StringValue... stringExprs) {
        super(functionType, stringExprs);
    }

    public Class<? extends ValueExpr> validateType() throws HPersistException {

        switch (this.getFunctionType()) {
            case TRIM: {
                final Class<? extends ValueExpr> type1 = this.getStringExprs()[0].validateType();
                if (!ExprTree.isOfType(type1, StringValue.class))
                    throw new HPersistException("Type " + type1.getName() + " not valid in TRIM");
                break;
            }

            case LOWER: {
                final Class<? extends ValueExpr> type1 = this.getStringExprs()[0].validateType();
                if (!ExprTree.isOfType(type1, StringValue.class))
                    throw new HPersistException("Type " + type1.getName() + " not valid in LOWER");
                break;
            }

            case UPPER: {
                final Class<? extends ValueExpr> type1 = this.getStringExprs()[0].validateType();
                if (!ExprTree.isOfType(type1, StringValue.class))
                    throw new HPersistException("Type " + type1.getName() + " not valid in UPPER");
                break;
            }

            case CONCAT: {
                final Class<? extends ValueExpr> type1 = this.getStringExprs()[0].validateType();
                final Class<? extends ValueExpr> type2 = this.getStringExprs()[1].validateType();
                if (!ExprTree.isOfType(type1, StringValue.class))
                    throw new HPersistException("Type " + type1.getName() + " not valid in CONCAT");
                if (!ExprTree.isOfType(type2, StringValue.class))
                    throw new HPersistException("Type " + type2.getName() + " not valid in CONCAT");
                break;
            }

            case REPLACE: {
                final Class<? extends ValueExpr> type1 = this.getStringExprs()[0].validateType();
                final Class<? extends ValueExpr> type2 = this.getStringExprs()[1].validateType();
                final Class<? extends ValueExpr> type3 = this.getStringExprs()[2].validateType();
                if (!ExprTree.isOfType(type1, StringValue.class))
                    throw new HPersistException("Type " + type1.getName() + " not valid in REPLACE");
                if (!ExprTree.isOfType(type2, StringValue.class))
                    throw new HPersistException("Type " + type2.getName() + " not valid in REPLACE");
                if (!ExprTree.isOfType(type3, StringValue.class))
                    throw new HPersistException("Type " + type3.getName() + " not valid in REPLACE");
                break;
            }

            default:
                throw new HPersistException("Error in StringFunction.validateType() " + this.getFunctionType());
        }

        return StringValue.class;
    }

    @Override
    public String getValue(final Object object) throws HPersistException {

        switch (this.getFunctionType()) {
            case TRIM: {
                final String val = this.getStringExprs()[0].getValue(object);
                return val.trim();
            }

            case LOWER: {
                final String val = this.getStringExprs()[0].getValue(object);
                return val.toLowerCase();
            }

            case UPPER: {
                final String val = this.getStringExprs()[0].getValue(object);
                return val.toUpperCase();
            }

            case CONCAT: {
                final String v1 = this.getStringExprs()[0].getValue(object);
                final String v2 = this.getStringExprs()[1].getValue(object);
                return v1 + v2;
            }

            case REPLACE: {
                final String v1 = this.getStringExprs()[0].getValue(object);
                final String v2 = this.getStringExprs()[1].getValue(object);
                final String v3 = this.getStringExprs()[2].getValue(object);
                return v1.replace(v2, v3);
            }

            default:
                throw new HPersistException("Error in StringFunction.getValue() " + this.getFunctionType());
        }
    }
}

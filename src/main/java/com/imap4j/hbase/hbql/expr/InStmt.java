package com.imap4j.hbase.hbql.expr;

import com.imap4j.hbase.hbql.HPersistException;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class InStmt implements PredicateExpr {


    private final ExprType type;
    private final ValueExpr expr;
    private final boolean not;
    private final List<Object> valList;

    public InStmt(final ExprType type, final ValueExpr expr, final boolean not, final List<Object> valList) {
        this.type = type;
        this.expr = expr;
        this.not = not;
        this.valList = valList;
    }

    @Override
    public boolean evaluate(final AttribContext context) throws HPersistException {

        final boolean retval = this.evaluateList(context);
        return (this.not) ? !retval : retval;
    }

    private boolean evaluateList(final AttribContext context) throws HPersistException {

        switch (type) {

            case IntegerType: {
                final Number number = (Number)this.expr.getValue(context);
                final int attribVal = number.intValue();
                for (final Object obj : this.valList) {
                    final Number numobj = (Number)((ValueExpr)obj).getValue(context);
                    final int val = numobj.intValue();
                    if (attribVal == val)
                        return true;
                }
                return false;

            }

            case StringType: {
                final String attribVal = (String)this.expr.getValue(context);
                for (final Object obj : this.valList) {
                    final String val = (String)((ValueExpr)obj).getValue(context);
                    if (attribVal.equals(val))
                        return true;
                }
                return false;

            }
            default:
                throw new HPersistException("Unknown type in InStmt.evaluateList() - " + type);

        }
    }
}
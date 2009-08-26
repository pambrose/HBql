package com.imap4j.hbase.hql.expr;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 8:23:42 PM
 */
public class AndExpr implements Evaluatable {
    public CondFactor expr1;
    public AndExpr expr2;

    @Override
    public boolean evaluate() {
        if (expr2 == null)
            return expr1.evaluate();
        else
            return expr1.evaluate() && expr2.evaluate();
    }
}

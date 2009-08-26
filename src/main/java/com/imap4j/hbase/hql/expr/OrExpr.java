package com.imap4j.hbase.hql.expr;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class OrExpr implements Evaluatable {

    public AndExpr expr1;
    public OrExpr expr2;

    @Override
    public boolean evaluate() {
        if (expr2 == null)
            return expr1.evaluate();
        else
            return expr1.evaluate() || expr2.evaluate();
    }
}

package com.imap4j.hbase.hql.expr;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 8:23:42 PM
 */
public class CondTerm implements Evaluatable {
    public CondFactor factor;
    public CondTerm term;

    @Override
    public boolean evaluate() {
        return false;
    }
}

package com.imap4j.hbase.hbql.expr.predicate;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 31, 2009
 * Time: 2:00:25 PM
 */
public abstract class GenericNotStmt {

    private final boolean not;

    protected GenericNotStmt(final boolean not) {
        this.not = not;
    }

    public boolean isNot() {
        return not;
    }

}
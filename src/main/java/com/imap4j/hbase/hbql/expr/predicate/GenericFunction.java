package com.imap4j.hbase.hbql.expr.predicate;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 31, 2009
 * Time: 2:00:25 PM
 */
public abstract class GenericFunction {

    public enum FUNC {
        CONCAT,
        TRIM,
        LOWER,
        UPPER
    }

    private final GenericFunction.FUNC func;

    protected GenericFunction(final FUNC func) {
        this.func = func;
    }

    protected GenericFunction.FUNC getFunc() {
        return this.func;
    }

}
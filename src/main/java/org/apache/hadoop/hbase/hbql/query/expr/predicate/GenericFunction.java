package org.apache.hadoop.hbase.hbql.query.expr.predicate;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 31, 2009
 * Time: 2:00:25 PM
 */
public abstract class GenericFunction {

    public enum Func {
        CONCAT,
        TRIM,
        LOWER,
        UPPER
    }

    private final Func func;

    protected GenericFunction(final Func func) {
        this.func = func;
    }

    protected Func getFunc() {
        return this.func;
    }

}
package com.imap4j.hbase.hql.expr;

import com.imap4j.hbase.hql.ClassSchema;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class IntInExpr extends InExpr implements Evaluatable {

    private final List<Integer> intList;

    public IntInExpr(final String attrib, final boolean not, final List<Integer> intList) {
        super(attrib, not);
        this.intList = intList;
    }

    @Override
    public boolean evaluate(final ClassSchema classSchema, final Object recordObj) {
        return false;
    }
}
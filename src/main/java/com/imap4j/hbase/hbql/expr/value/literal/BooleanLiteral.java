package com.imap4j.hbase.hbql.expr.value.literal;

import com.imap4j.hbase.hbql.HPersistException;
import com.imap4j.hbase.hbql.expr.EvalContext;
import com.imap4j.hbase.hbql.expr.node.BooleanValue;
import com.imap4j.hbase.hbql.expr.node.PredicateExpr;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class BooleanLiteral extends GenericLiteral implements BooleanValue, PredicateExpr {

    private final Boolean value;

    public BooleanLiteral(final String text) {
        this.value = text.equalsIgnoreCase("true");
    }

    public BooleanLiteral(final boolean value) {
        this.value = value;
    }

    @Override
    public Boolean getValue(final EvalContext context) {
        return this.value;
    }

    @Override
    public Boolean evaluate(final EvalContext context) throws HPersistException {
        return this.value;
    }

}
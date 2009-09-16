package com.imap4j.hbase.collection;

import com.google.common.base.Predicate;
import com.imap4j.hbase.antlr.config.HBqlRule;
import com.imap4j.hbase.hbase.HPersistException;
import com.imap4j.hbase.hbql.expr.ExprTree;
import com.imap4j.hbase.hbql.schema.HUtil;
import com.imap4j.hbase.hbql.schema.ObjectSchema;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 9, 2009
 * Time: 10:09:38 PM
 */
public class ObjectQueryPredicate<T> implements Predicate<T> {

    private final String query;
    private ExprTree tree;
    private boolean initialized = false;

    public ObjectQueryPredicate(final String query) {
        this.query = query;
    }

    @Override
    public boolean apply(final T obj) {

        try {
            if (!initialized) {
                final ObjectSchema schema = ObjectSchema.getObjectSchema(obj);
                this.tree = HUtil.parseExprTree(HBqlRule.NODESC_WHERE_EXPR, this.query, schema, true);
                initialized = true;
            }

            return tree.evaluate(obj);
        }
        catch (HPersistException e) {
            e.printStackTrace();
            return false;
        }
    }
}

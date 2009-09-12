package com.imap4j.hbase.collection;

import com.google.common.base.Predicate;
import com.imap4j.hbase.antlr.config.HBqlRule;
import com.imap4j.hbase.hbase.HPersistException;
import com.imap4j.hbase.hbql.expr.ExprTree;
import com.imap4j.hbase.hbql.schema.ObjectSchema;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 9, 2009
 * Time: 10:09:38 PM
 */
public class ObjectQueryPredicate<T> implements Predicate<T> {

    private final String query;
    private ObjectSchema schema;
    private ExprTree tree;
    private boolean initialized = false;

    public ObjectQueryPredicate(final String query) {
        this.query = query;
    }

    @Override
    public boolean apply(final T obj) {

        try {
            if (!initialized) {
                this.schema = ObjectSchema.getObjectSchema(obj);
                this.tree = (ExprTree)HBqlRule.NODESC_WHERE_EXPR.parse(this.query, schema);
                this.tree.setSchema(schema);
                this.tree.optimize();
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

package com.imap4j.hbase.hbql.test;

import com.imap4j.hbase.antlr.config.HBqlRule;
import com.imap4j.hbase.hbql.HPersistException;
import com.imap4j.hbase.hbql.expr.ReflectionEvalContext;
import com.imap4j.hbase.hbql.expr.predicate.ExprEvalTree;
import com.imap4j.hbase.hbql.schema.ObjectSchema;

import java.util.Collection;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 23, 2009
 * Time: 4:49:02 PM
 */
public class ObjectTests {

    protected void assertResultCount(final Collection<?> objList, final String expr, final int expected_cnt) throws HPersistException {

        if (objList == null || objList.size() == 0) {
            org.junit.Assert.assertTrue(expected_cnt == 0);
            return;
        }

        final Object obj = objList.iterator().next();
        ObjectSchema schema = ObjectSchema.getObjectSchema(obj);

        final ExprEvalTree tree = (ExprEvalTree)HBqlRule.NODESC_WHERE_EXPR.parse(expr, schema);
        tree.optimizeForConstants(new ReflectionEvalContext(schema, null));

        int cnt = 0;
        for (final Object o : objList) {
            if (tree.evaluate(new ReflectionEvalContext(schema, o)))
                cnt++;
        }

        System.out.println("Count = " + cnt);
        org.junit.Assert.assertTrue(expected_cnt == cnt);
    }

}
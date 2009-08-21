package com.imap4j.hbase.hbql;

import com.imap4j.hbase.antlr.QueryArgs;
import com.imap4j.hbase.antlr.config.HBqlRule;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 20, 2009
 * Time: 9:26:38 PM
 */
public class Query<T extends Persistable> {

    final String query;
    final QueryListener<T> listener;

    public Query(final String query, final QueryListener<T> listener) {
        this.query = query;
        this.listener = listener;
    }

    public void execute() throws PersistException {

        final QueryArgs qa = (QueryArgs)HBqlRule.QUERY.parse(this.query);

        final Class tableClass;
        try {
            tableClass = Class.forName(qa.getTableName());
        }
        catch (ClassNotFoundException e) {
            throw new PersistException("Cannot find class: " + qa.getTableName());
        }

        final ClassSchema classSchema = ClassSchema.getClassSchema(tableClass);

        int t = 0;
    }
}

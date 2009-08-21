package com.imap4j.hbase.hbsql;

import com.imap4j.hbase.antlr.config.HBSqlRule;

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

    public void execute() {
        HBSqlRule.QUERY.parse(this.query);

    }
}

package com.imap4j.hbase.hbase;

import com.imap4j.hbase.antlr.args.QueryArgs;
import com.imap4j.hbase.antlr.args.WhereArgs;
import com.imap4j.hbase.antlr.config.HBqlRule;
import com.imap4j.hbase.hbql.expr.ExprTree;
import com.imap4j.hbase.hbql.schema.ExprSchema;
import com.imap4j.hbase.hbql.schema.HBaseSchema;
import com.imap4j.hbase.util.Lists;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.HBqlFilter;

import java.io.IOException;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 20, 2009
 * Time: 9:26:38 PM
 */
public class HQuery<T> {

    final HConnection connection;
    final String query;
    final HBaseSchema schema;
    final List<String> fieldList;
    final ExprTree clientExprTree;
    final List<Scan> scanList;

    private List<HQueryListener<T>> listeners = null;

    public HQuery(final HConnection connection, final String query) throws IOException, HPersistException {

        this.connection = connection;
        this.query = query;

        final QueryArgs args = (QueryArgs)HBqlRule.SELECT.parse(this.getQuery(), (ExprSchema)null);
        this.schema = (HBaseSchema)args.getSchema();

        final WhereArgs where = args.getWhereExpr();

        this.fieldList = (args.getColumns() == null) ? this.getSchema().getFieldList() : args.getColumns();

        this.clientExprTree = where.getClientExprTree().setSchema(this.getSchema(), this.getFieldList());

        final HBqlFilter serverFilter = this.getSchema().getHBqlFilter(where.getServerExprTree(),
                                                                       this.getFieldList(),
                                                                       where.getLimitArgs());

        this.scanList = this.getSchema().getScanList(this.getFieldList(),
                                                     where.getKeyRangeArgs(),
                                                     where.getDateRangeArgs(),
                                                     where.getVersionArgs(),
                                                     serverFilter);
    }

    public synchronized void addListener(final HQueryListener<T> listener) {
        if (this.getListeners() == null)
            this.listeners = Lists.newArrayList();

        this.getListeners().add(listener);
    }

    List<Scan> getScanList() {
        return this.scanList;
    }

    HBaseSchema getSchema() {
        return this.schema;
    }

    ExprTree getClientExprTree() {
        return this.clientExprTree;
    }

    List<String> getFieldList() {
        return this.fieldList;
    }

    public String getQuery() {
        return this.query;
    }

    HConnection getConnection() {
        return this.connection;
    }

    public List<HQueryListener<T>> getListeners() {
        return this.listeners;
    }

    public void clearListeners() {
        if (this.getListeners() != null)
            this.getListeners().clear();
    }


    public HResults<T> execute() throws IOException, HPersistException {

        if (this.getListeners() != null) {
            for (final HQueryListener<T> listener : this.getListeners())
                listener.onQueryInit();
        }

        return new HResults<T>(this);
    }
}

package org.apache.hadoop.hbase.hbql.client;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.HBqlFilter;
import org.apache.hadoop.hbase.hbql.query.antlr.HBql;
import org.apache.hadoop.hbase.hbql.query.antlr.args.QueryArgs;
import org.apache.hadoop.hbase.hbql.query.antlr.args.WhereArgs;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.query.schema.HBaseSchema;
import org.apache.hadoop.hbase.hbql.query.schema.VariableAttrib;
import org.apache.hadoop.hbase.hbql.query.util.Lists;

import java.io.IOException;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 20, 2009
 * Time: 9:26:38 PM
 */
public class HQuery<T> {

    private final HConnection connection;
    private final String query;
    private final QueryArgs queryArgs;
    private final List<Scan> scanList;

    private List<HQueryListener<T>> listeners = null;

    public HQuery(final HConnection connection, final String query) throws IOException, HBqlException {

        this.connection = connection;
        this.query = query;

        this.queryArgs = HBql.parseQuery(this.getQuery());

        this.getWhereArgs().setSchema(this.getSchema());
        this.getWhereArgs().validateTypes();
        this.getWhereArgs().optimize();

        this.getClientExprTree().validate(this.getSelectAttribList());

        final HBqlFilter serverFilter = this.getSchema().getHBqlFilter(this.getWhereArgs().getServerExprTree(),
                                                                       this.getSelectAttribList(),
                                                                       this.getWhereArgs().getScanLimit());

        this.scanList = this.getSchema().getScanList(this.getSelectAttribList(),
                                                     this.getWhereArgs().getKeyRangeArgs(),
                                                     this.getWhereArgs().getTimeRangeArgs(),
                                                     this.getWhereArgs().getVersionArgs(),
                                                     serverFilter);
    }

    public synchronized void addListener(final HQueryListener<T> listener) {
        if (this.getListeners() == null)
            this.listeners = Lists.newArrayList();

        this.getListeners().add(listener);
    }

    private QueryArgs getQueryArgs() {
        return this.queryArgs;
    }

    private WhereArgs getWhereArgs() {
        return this.getQueryArgs().getWhereExpr();
    }

    List<Scan> getScanList() {
        return this.scanList;
    }

    HBaseSchema getSchema() {
        return this.getQueryArgs().getSchema();
    }

    ExprTree getClientExprTree() {
        return this.getWhereArgs().getClientExprTree();
    }

    List<VariableAttrib> getSelectAttribList() {
        return this.getQueryArgs().getSelectVariableAttribList();
    }

    public String getQuery() {
        return this.query;
    }

    public long getQueryLimit() throws HBqlException {
        return this.getQueryArgs().getWhereExpr().getQueryLimit();
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


    public HResults<T> execute() throws IOException, HBqlException {

        if (this.getListeners() != null) {
            for (final HQueryListener<T> listener : this.getListeners())
                listener.onQueryInit();
        }

        return new HResults<T>(this);
    }
}

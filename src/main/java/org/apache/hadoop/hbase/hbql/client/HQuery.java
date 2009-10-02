package org.apache.hadoop.hbase.hbql.client;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.HBqlFilter;
import org.apache.hadoop.hbase.hbql.query.antlr.HBql;
import org.apache.hadoop.hbase.hbql.query.antlr.args.QueryArgs;
import org.apache.hadoop.hbase.hbql.query.antlr.args.SelectElement;
import org.apache.hadoop.hbase.hbql.query.antlr.args.WhereArgs;
import org.apache.hadoop.hbase.hbql.query.schema.ColumnAttrib;
import org.apache.hadoop.hbase.hbql.query.schema.HBaseSchema;
import org.apache.hadoop.hbase.hbql.query.util.Lists;
import org.apache.hadoop.hbase.hbql.query.util.Sets;

import java.io.IOException;
import java.util.List;
import java.util.Set;

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

        final WhereArgs where = this.getWhereArgs();

        where.setSchema(this.getSchema());
        where.validateTypes();
        where.optimize();

        where.getClientExprTree().validate();

        // Get list of all columns that are used in select list and expr tree
        final Set<ColumnAttrib> allAttribs = Sets.newHashSet();
        allAttribs.addAll(this.getQueryArgs().getSelectAttribList());
        allAttribs.addAll(where.getAllColumnsUsedInExprs());

        final HBqlFilter serverFilter = this.getSchema().getHBqlFilter(where.getServerExprTree(),
                                                                       where.getScanLimit());

        this.scanList = this.getSchema().getScanList(allAttribs,
                                                     where.getKeyRangeArgs(),
                                                     where.getTimeRangeArgs(),
                                                     where.getVersionArgs(),
                                                     serverFilter);
    }

    public synchronized void addListener(final HQueryListener<T> listener) {
        if (this.getListeners() == null)
            this.listeners = Lists.newArrayList();

        this.getListeners().add(listener);
    }

    HConnection getConnection() {
        return this.connection;
    }

    public String getQuery() {
        return this.query;
    }

    public QueryArgs getQueryArgs() {
        return this.queryArgs;
    }

    public WhereArgs getWhereArgs() {
        return this.getQueryArgs().getWhereArgs();
    }

    List<Scan> getScanList() {
        return this.scanList;
    }

    public List<HQueryListener<T>> getListeners() {
        return this.listeners;
    }

    HBaseSchema getSchema() {
        return this.getQueryArgs().getSchema();
    }

    List<SelectElement> getSelectElementList() {
        return this.getQueryArgs().getSelectElementList();
    }

    public long getQueryLimit() throws HBqlException {
        return this.getQueryArgs().getWhereArgs().getQueryLimit();
    }

    public void clearListeners() {
        if (this.getListeners() != null)
            this.getListeners().clear();
    }

    public HResults<T> execute() throws HBqlException {

        if (this.getListeners() != null) {
            for (final HQueryListener<T> listener : this.getListeners())
                listener.onQueryInit();
        }

        return new HResults<T>(this);
    }
}

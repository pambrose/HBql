package com.imap4j.hbase.antlr.args;

import com.imap4j.hbase.hbql.expr.ExprTree;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 4, 2009
 * Time: 10:29:16 AM
 */
public class WhereArgs {

    private KeyRangeArgs keyRange = new KeyRangeArgs(null);
    private DateRangeArgs dateRange = new DateRangeArgs(null, null);
    private VersionArgs version = new VersionArgs(null);
    private ExprTree clientFilter = ExprTree.newExprTree(null);
    private ExprTree serverFilter = ExprTree.newExprTree(null);

    public KeyRangeArgs getKeyRange() {
        return this.keyRange;
    }

    public void setKeyRange(final KeyRangeArgs keyRange) {
        this.keyRange = keyRange;
    }

    public DateRangeArgs getDateRange() {
        return this.dateRange;
    }

    public void setDateRange(final DateRangeArgs dateRange) {
        this.dateRange = dateRange;
    }

    public VersionArgs getVersion() {
        return this.version;
    }

    public void setVersion(final VersionArgs version) {
        this.version = version;
    }

    public ExprTree getClientFilter() {
        return clientFilter;
    }

    public void setClientFilter(final ExprTree clientFilter) {
        this.clientFilter = clientFilter;
    }

    public ExprTree getServerFilter() {
        return serverFilter;
    }

    public void setServerFilter(final ExprTree serverFilter) {
        this.serverFilter = serverFilter;
    }
}

package com.imap4j.hbase.antlr.args;

import com.imap4j.hbase.hbql.expr.ExprTree;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 4, 2009
 * Time: 10:29:16 AM
 */
public class WhereArgs {

    private KeyRangeArgs keyRangeArgs = new KeyRangeArgs(null);
    private DateRangeArgs dateRangeArgs = new DateRangeArgs(null, null);
    private VersionArgs versionArgs = new VersionArgs(null);
    private LimitArgs limitArgs = new LimitArgs(null);
    private ExprTree clientFilter = ExprTree.newExprTree(null);
    private ExprTree serverFilter = ExprTree.newExprTree(null);

    public KeyRangeArgs getKeyRangeArgs() {
        return this.keyRangeArgs;
    }

    public void setKeyRangeArgs(final KeyRangeArgs keyRange) {
        this.keyRangeArgs = keyRange;
    }

    public DateRangeArgs getDateRangeArgs() {
        return this.dateRangeArgs;
    }

    public void setDateRangeArgs(final DateRangeArgs dateRange) {
        this.dateRangeArgs = dateRange;
    }

    public VersionArgs getVersionArgs() {
        return this.versionArgs;
    }

    public void setVersionArgs(final VersionArgs versionArgs) {
        this.versionArgs = versionArgs;
    }

    public LimitArgs getLimitArgs() {
        return limitArgs;
    }

    public void setLimitArgs(final LimitArgs limitArgs) {
        this.limitArgs = limitArgs;
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

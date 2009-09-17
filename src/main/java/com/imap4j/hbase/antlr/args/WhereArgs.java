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
    private LimitArgs scanLimitArgs = new LimitArgs(null);
    private LimitArgs queryLimitArgs = new LimitArgs(null);
    private ExprTree clientExprTree = ExprTree.newExprTree(null);
    private ExprTree serverExprTree = ExprTree.newExprTree(null);

    public KeyRangeArgs getKeyRangeArgs() {
        return this.keyRangeArgs;
    }

    public void setKeyRangeArgs(final KeyRangeArgs keyRangeArgs) {
        if (keyRangeArgs != null)
            this.keyRangeArgs = keyRangeArgs;
    }

    public DateRangeArgs getDateRangeArgs() {
        return this.dateRangeArgs;
    }

    public void setDateRangeArgs(final DateRangeArgs dateRangeArgs) {
        if (dateRangeArgs != null)
            this.dateRangeArgs = dateRangeArgs;
    }

    public VersionArgs getVersionArgs() {
        return this.versionArgs;
    }

    public void setVersionArgs(final VersionArgs versionArgs) {
        if (versionArgs != null)
            this.versionArgs = versionArgs;
    }

    public LimitArgs getScanLimitArgs() {
        return this.scanLimitArgs;
    }

    public void setScanLimitArgs(final LimitArgs scanLimitArgs) {
        if (scanLimitArgs != null)
            this.scanLimitArgs = scanLimitArgs;
    }

    public LimitArgs getQueryLimitArgs() {
        return this.queryLimitArgs;
    }

    public void setQueryLimitArgs(final LimitArgs queryLimitArgs) {
        if (queryLimitArgs != null)
            this.queryLimitArgs = queryLimitArgs;
    }

    public ExprTree getClientExprTree() {
        return this.clientExprTree;
    }

    public void setClientExprTree(final ExprTree clientExprTree) {
        if (clientExprTree != null)
            this.clientExprTree = clientExprTree;
    }

    public ExprTree getServerExprTree() {
        return serverExprTree;
    }

    public void setServerExprTree(final ExprTree serverExprTree) {
        if (serverExprTree != null)
            this.serverExprTree = serverExprTree;
    }

    public long getQueryLimit() {
        return (this.getQueryLimitArgs() != null) ? this.getQueryLimitArgs().getValue() : 0;
    }

    public long getScanLimit() {
        return (this.getScanLimitArgs() != null) ? this.getScanLimitArgs().getValue() : 0;
    }
}

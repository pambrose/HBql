package com.imap4j.hbase.antlr.args;

import com.imap4j.hbase.hbql.expr.predicate.ExprEvalTree;

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
    private ExprEvalTree clientFilterArgs = new ExprEvalTree(null);
    private ExprEvalTree serverFilterArgs = new ExprEvalTree(null);


    public KeyRangeArgs getKeyRangeArgs() {
        return this.keyRangeArgs;
    }

    public void setKeyRangeArgs(final KeyRangeArgs keyRangeArgs) {
        this.keyRangeArgs = keyRangeArgs;
    }

    public DateRangeArgs getDateRangeArgs() {
        return this.dateRangeArgs;
    }

    public void setDateRangeArgs(final DateRangeArgs dateRangeArgs) {
        this.dateRangeArgs = dateRangeArgs;
    }

    public VersionArgs getVersionArgs() {
        return this.versionArgs;
    }

    public void setVersionArgs(final VersionArgs versionArgs) {
        this.versionArgs = versionArgs;
    }

    public ExprEvalTree getClientFilterArgs() {
        return clientFilterArgs;
    }

    public void setClientFilterArgs(final ExprEvalTree clientFilterArgs) {
        this.clientFilterArgs = clientFilterArgs;
    }

    public ExprEvalTree getServerFilterArgs() {
        return serverFilterArgs;
    }

    public void setServerFilterArgs(final ExprEvalTree serverFilterArgs) {
        this.serverFilterArgs = serverFilterArgs;
    }
}

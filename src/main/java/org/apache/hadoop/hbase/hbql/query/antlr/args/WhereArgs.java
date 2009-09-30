package org.apache.hadoop.hbase.hbql.query.antlr.args;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.query.schema.HBaseSchema;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 4, 2009
 * Time: 10:29:16 AM
 */
public class WhereArgs {

    private KeyRangeArgs keyRangeArgs = new KeyRangeArgs(null);
    private TimeRangeArgs timeRangeArgs = new TimeRangeArgs(null, null);
    private VersionArgs versionArgs = new VersionArgs(null);
    private LimitArgs scanLimitArgs = new LimitArgs(null);
    private LimitArgs queryLimitArgs = new LimitArgs(null);
    private ExprTree clientExprTree = ExprTree.newExprTree(true, null);
    private ExprTree serverExprTree = ExprTree.newExprTree(true, null);

    public void setSchema(final HBaseSchema schema) {
        // this.getKeyRangeArgs().setSchema(null);
        this.getTimeRangeArgs().setSchema(null);
        this.getVersionArgs().setSchema(null);
        this.getScanLimitArgs().setSchema(null);
        this.getQueryLimitArgs().setSchema(null);
        this.getServerExprTree().setSchema(schema);
        this.getClientExprTree().setSchema(schema);
    }

    public void validateTypes() throws HBqlException {
        // this.getKeyRangeArgs().validateTypes();
        this.getTimeRangeArgs().validateTypes(false);
        this.getVersionArgs().validateTypes(false);
        this.getScanLimitArgs().validateTypes(false);
        this.getQueryLimitArgs().validateTypes(false);
        //this.getServerExprTree().validateTypes(true);
        //this.getClientExprTree().validateTypes(true);
    }

    public void optimize() throws HBqlException {
        // this.getKeyRangeArgs().optimize();
        this.getTimeRangeArgs().optimize();
        this.getVersionArgs().optimize();
        this.getScanLimitArgs().optimize();
        this.getQueryLimitArgs().optimize();
        //this.getServerExprTree().optimize();
        //this.getClientExprTree().optimize();
    }

    public KeyRangeArgs getKeyRangeArgs() {
        return this.keyRangeArgs;
    }

    public void setKeyRangeArgs(final KeyRangeArgs keyRangeArgs) {
        if (keyRangeArgs != null)
            this.keyRangeArgs = keyRangeArgs;
    }

    public TimeRangeArgs getTimeRangeArgs() {
        return this.timeRangeArgs;
    }

    public void setTimeRangeArgs(final TimeRangeArgs timeRangeArgs) {
        if (timeRangeArgs != null)
            this.timeRangeArgs = timeRangeArgs;
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

    public long getQueryLimit() throws HBqlException {
        return (this.getQueryLimitArgs() != null && this.getQueryLimitArgs().isValid())
               ? this.getQueryLimitArgs().getValue() : 0;
    }

    public long getScanLimit() throws HBqlException {
        return (this.getScanLimitArgs() != null && this.getScanLimitArgs().isValid())
               ? this.getScanLimitArgs().getValue() : 0;
    }

    public String asString() {

        final StringBuilder sbuf = new StringBuilder("WITH ");

        if (this.getKeyRangeArgs().isValid())
            sbuf.append(this.getKeyRangeArgs().asString() + "\n");

        if (this.getTimeRangeArgs().isValid())
            sbuf.append(this.getTimeRangeArgs().asString() + "\n");

        if (this.getVersionArgs().isValid())
            sbuf.append(this.getVersionArgs().asString() + "\n");

        if (this.getScanLimitArgs().isValid())
            sbuf.append("SCAN " + this.getScanLimitArgs().asString() + "\n");

        if (this.getQueryLimitArgs().isValid())
            sbuf.append("QUERY " + this.getQueryLimitArgs().asString() + "\n");

        if (this.getServerExprTree().isValid())
            sbuf.append("SERVER FILTER " + this.getServerExprTree().asString() + "\n");

        if (this.getClientExprTree().isValid())
            sbuf.append("CLIENT FILTER " + this.getClientExprTree().asString() + "\n");

        return sbuf.toString();
    }
}

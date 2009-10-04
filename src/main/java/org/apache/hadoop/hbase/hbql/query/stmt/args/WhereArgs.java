package org.apache.hadoop.hbase.hbql.query.stmt.args;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.HBqlFilter;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.query.schema.ColumnAttrib;
import org.apache.hadoop.hbase.hbql.query.schema.HBaseSchema;
import org.apache.hadoop.hbase.hbql.query.util.Lists;
import org.apache.hadoop.hbase.hbql.query.util.Sets;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 4, 2009
 * Time: 10:29:16 AM
 */
public class WhereArgs {

    private KeyRangeArgs keyRangeArgs = null;
    private TimeRangeArgs timeRangeArgs = null;
    private VersionArgs versionArgs = null;
    private LimitArgs scanLimitArgs = null;
    private LimitArgs queryLimitArgs = null;

    private ExprTree clientExprTree = null;
    private ExprTree serverExprTree = null;

    private HBaseSchema schema;

    public void setSchema(final HBaseSchema schema) throws HBqlException {

        this.schema = schema;

        if (this.getKeyRangeArgs() != null)
            this.getKeyRangeArgs().setSchema(null);

        if (this.getTimeRangeArgs() != null)
            this.getTimeRangeArgs().setSchema(null);

        if (this.getVersionArgs() != null)
            this.getVersionArgs().setSchema(null);

        if (this.getScanLimitArgs() != null)
            this.getScanLimitArgs().setSchema(null);

        if (this.getQueryLimitArgs() != null)
            this.getQueryLimitArgs().setSchema(null);

        if (this.getServerExprTree() != null)
            this.getServerExprTree().setSchema(this.getSchema());

        if (this.getClientExprTree() != null)
            this.getClientExprTree().setSchema(this.getSchema());
    }

    private HBaseSchema getSchema() {
        return this.schema;
    }

    private KeyRangeArgs getKeyRangeArgs() {
        return this.keyRangeArgs;
    }

    public void setKeyRangeArgs(final KeyRangeArgs keyRangeArgs) {
        if (keyRangeArgs != null)
            this.keyRangeArgs = keyRangeArgs;
    }

    private TimeRangeArgs getTimeRangeArgs() {
        return this.timeRangeArgs;
    }

    public void setTimeRangeArgs(final TimeRangeArgs timeRangeArgs) {
        if (timeRangeArgs != null)
            this.timeRangeArgs = timeRangeArgs;
    }

    private VersionArgs getVersionArgs() {
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

        if (this.getKeyRangeArgs() != null && this.getKeyRangeArgs().isValid())
            sbuf.append(this.getKeyRangeArgs().asString() + "\n");

        if (this.getTimeRangeArgs() != null && this.getTimeRangeArgs().isValid())
            sbuf.append(this.getTimeRangeArgs().asString() + "\n");

        if (this.getVersionArgs() != null && this.getVersionArgs().isValid())
            sbuf.append(this.getVersionArgs().asString() + "\n");

        if (this.getScanLimitArgs() != null && this.getScanLimitArgs().isValid())
            sbuf.append("SCAN " + this.getScanLimitArgs().asString() + "\n");

        if (this.getQueryLimitArgs() != null && this.getQueryLimitArgs().isValid())
            sbuf.append("QUERY " + this.getQueryLimitArgs().asString() + "\n");

        if (this.getServerExprTree() != null && this.getServerExprTree().isValid())
            sbuf.append("SERVER FILTER " + this.getServerExprTree().asString() + "\n");

        if (this.getClientExprTree() != null && this.getClientExprTree().isValid())
            sbuf.append("CLIENT FILTER " + this.getClientExprTree().asString() + "\n");

        return sbuf.toString();
    }

    public Set<ColumnAttrib> getAllColumnsUsedInExprs() {
        final Set<ColumnAttrib> allAttribs = Sets.newHashSet();
        if (this.getServerExprTree() != null)
            allAttribs.addAll(this.getServerExprTree().getAttribsUsedInExpr());
        if (this.getClientExprTree() != null)
            allAttribs.addAll(this.getClientExprTree().getAttribsUsedInExpr());
        return allAttribs;
    }

    public List<Scan> getScanList(final Collection<ColumnAttrib> columnAttribSet) throws IOException, HBqlException {

        final List<Scan> scanList = Lists.newArrayList();

        final KeyRangeArgs keyRangeArgs = this.getKeyRangeArgs();

        if (keyRangeArgs != null) {
            for (final KeyRangeArgs.Range range : keyRangeArgs.getRangeList())
                scanList.add(range.getScan());
        }

        // If nothing present, then scan all rows
        if (scanList.size() == 0)
            scanList.add(new Scan());

        for (final Scan scan : scanList) {

            // Set column names
            for (final ColumnAttrib attrib : columnAttribSet) {

                // Do not bother to request because it will always be delivered
                if (attrib.isKeyAttrib())
                    continue;

                // If it is a map, then request all columns for family
                if (attrib.isAFamilyAttrib() || attrib.isMapKeysAsColumns())
                    scan.addFamily(attrib.getFamilyNameAsBytes());
                else
                    scan.addColumn(attrib.getFamilyNameAsBytes(), attrib.getColumnNameAsBytes());
            }

            if (this.getTimeRangeArgs() != null && this.getTimeRangeArgs().isValid())
                this.getTimeRangeArgs().setTimeStamp(scan);

            if (this.getVersionArgs() != null && this.getVersionArgs().isValid())
                this.getVersionArgs().setMaxVersions(scan);

            final HBqlFilter serverFilter = this.getSchema().getHBqlFilter(this.getServerExprTree(),
                                                                           this.getScanLimit());
            if (serverFilter != null)
                scan.setFilter(serverFilter);
        }

        return scanList;
    }

}

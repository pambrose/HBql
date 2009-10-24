package org.apache.hadoop.hbase.hbql.stmt.args;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.InternalErrorException;
import org.apache.hadoop.hbase.hbql.client.ResultMissingColumnException;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.schema.ColumnAttrib;
import org.apache.hadoop.hbase.hbql.query.schema.Schema;
import org.apache.hadoop.hbase.hbql.query.util.HUtil;
import org.apache.hadoop.hbase.hbql.query.util.Lists;
import org.apache.hadoop.hbase.hbql.stmt.select.RowRequest;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

public class KeyRangeArgs {

    private final List<Range> rangeList;

    private enum Type {
        SINGLE, RANGE, LAST, ALL
    }

    public static class Range extends SelectArgs {
        private final KeyRangeArgs.Type type;

        private Range() {
            super(SelectArgs.Type.NOARGSKEY);
            this.type = KeyRangeArgs.Type.ALL;
        }

        private Range(final KeyRangeArgs.Type type, final GenericValue arg0) {
            super(SelectArgs.Type.SINGLEKEY, arg0);
            this.type = type;
        }

        private Range(final GenericValue arg0, final GenericValue arg1) {
            super(SelectArgs.Type.KEYRANGE, arg0, arg1);
            this.type = KeyRangeArgs.Type.RANGE;
        }

        private Object getLower(final boolean allowsCollections) throws HBqlException {
            return this.evaluateConstant(0, allowsCollections, null);
        }

        private String getUpper() throws HBqlException {
            return (String)this.evaluateConstant(1, false, null);
        }

        private KeyRangeArgs.Type getType() {
            return this.type;
        }

        private byte[] getUpperAsBytes() throws HBqlException {
            final String upper = this.getUpper();
            return HUtil.getSerialization().getStringAsBytes(upper);
        }

        private boolean isLastRange() {
            return this.getType() == KeyRangeArgs.Type.LAST;
        }

        public boolean isSingleKey() {
            return this.getType() == KeyRangeArgs.Type.SINGLE;
        }

        private boolean isRowRange() {
            return this.getType() == KeyRangeArgs.Type.RANGE;
        }

        public boolean isAllRows() {
            return this.getType() == KeyRangeArgs.Type.ALL;
        }

        public String asString() {
            final StringBuilder sbuf = new StringBuilder();

            if (this.isAllRows()) {
                sbuf.append("ALL");
            }
            else if (this.isSingleKey()) {
                sbuf.append("'" + this.getGenericValue(0).asString() + "'");
            }
            else {
                sbuf.append("'" + this.getGenericValue(0).asString() + "' TO ");
                if (this.isLastRange())
                    sbuf.append("LAST");
                else
                    sbuf.append("'" + this.getGenericValue(1).asString() + "'");
            }
            return sbuf.toString();
        }

        private RowRequest newGet(final WhereArgs whereArgs,
                                  final Collection<ColumnAttrib> columnAttribSet,
                                  final String lower) throws HBqlException, IOException {
            final byte[] lowerBytes = HUtil.getSerialization().getStringAsBytes(lower);
            final Get get = new Get(lowerBytes);
            whereArgs.setGetArgs(get, columnAttribSet);
            return new RowRequest(get, null);
        }

        public List<RowRequest> getGet(final WhereArgs whereArgs,
                                       final Collection<ColumnAttrib> columnAttribSet) throws HBqlException, IOException {

            List<RowRequest> retval = Lists.newArrayList();

            // Check if the value returned is a collection
            final Object objval = this.getLower(true);
            if (HUtil.isACollection(objval)) {
                for (final GenericValue val : (Collection<GenericValue>)objval) {
                    try {
                        final String lower = (String)val.getValue(null);
                        retval.add(this.newGet(whereArgs, columnAttribSet, lower));
                    }
                    catch (ResultMissingColumnException e) {
                        throw new InternalErrorException(val.asString());
                    }
                }
            }
            else {
                final String lower = (String)objval;
                retval.add(this.newGet(whereArgs, columnAttribSet, lower));
            }

            return retval;
        }

        public RowRequest getScan(final WhereArgs whereArgs,
                                  final Collection<ColumnAttrib> columnAttribSet) throws HBqlException, IOException {
            final Scan scan = new Scan();
            if (!this.isAllRows()) {
                final byte[] lowerBytes = HUtil.getSerialization().getStringAsBytes((String)this.getLower(false));
                scan.setStartRow(lowerBytes);
                if (this.isRowRange())
                    scan.setStopRow(this.getUpperAsBytes());
            }
            whereArgs.setScanArgs(scan, columnAttribSet);
            return new RowRequest(null, scan);
        }

        public void process(final WhereArgs whereArgs,
                            final List<RowRequest> rowRequestList,
                            final Collection<ColumnAttrib> columnAttribSet) throws HBqlException, IOException {

            if (this.isSingleKey())
                rowRequestList.addAll(this.getGet(whereArgs, columnAttribSet));
            else
                rowRequestList.add(this.getScan(whereArgs, columnAttribSet));
        }
    }

    public KeyRangeArgs() {
        this(null);
    }

    public KeyRangeArgs(final List<Range> rangeList) {
        if (rangeList == null) {
            this.rangeList = Lists.newArrayList();
            this.getRangeList().add(newAllRange());
        }
        else {
            this.rangeList = rangeList;
        }
    }

    public static Range newRange(final GenericValue arg0, final GenericValue arg1) {
        return new Range(arg0, arg1);
    }

    public static Range newSingleKey(final GenericValue arg0) {
        return new Range(KeyRangeArgs.Type.SINGLE, arg0);
    }

    public static Range newLastRange(final GenericValue arg0) {
        return new Range(KeyRangeArgs.Type.LAST, arg0);
    }

    public static Range newAllRange() {
        return new Range();
    }

    public List<Range> getRangeList() {
        return this.rangeList;
    }

    public void setSchema(final Schema schema) {
        for (final Range range : this.getRangeList())
            range.setSchema(schema);
    }

    public String asString() {
        final StringBuilder sbuf = new StringBuilder("KEYS ");
        boolean first = true;
        for (final Range range : this.getRangeList()) {
            if (!first)
                sbuf.append(", ");
            sbuf.append(range.asString());
            first = false;
        }
        return sbuf.toString();
    }

    public int setParameter(final String name, final Object val) throws HBqlException {
        int cnt = 0;
        for (final Range range : this.getRangeList())
            cnt += range.setParameter(name, val);
        return cnt;
    }
}

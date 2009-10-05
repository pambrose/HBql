package org.apache.hadoop.hbase.hbql.query.stmt.args;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.schema.ColumnAttrib;
import org.apache.hadoop.hbase.hbql.query.schema.Schema;
import org.apache.hadoop.hbase.hbql.query.util.HUtil;
import org.apache.hadoop.hbase.hbql.query.util.Lists;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 4, 2009
 * Time: 9:53:48 AM
 */
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

        public String getLower() throws HBqlException {
            return (String)this.evaluate(0, false, null);
        }

        public String getUpper() throws HBqlException {
            return (String)this.evaluate(1, false, null);
        }

        public KeyRangeArgs.Type getType() {
            return this.type;
        }

        public byte[] getLowerAsBytes() throws HBqlException {
            return HUtil.ser.getStringAsBytes(this.getLower());
        }

        public byte[] getUpperAsBytes() throws HBqlException {
            return HUtil.ser.getStringAsBytes(this.getUpper());
        }

        public boolean isLastRange() {
            return this.getType() == KeyRangeArgs.Type.LAST;
        }

        public String asString() {
            try {
                final StringBuilder sbuf = new StringBuilder();
                sbuf.append("'" + this.getLower() + "' TO ");
                if (this.isLastRange())
                    sbuf.append("LAST");
                else
                    sbuf.append("'" + this.getUpper() + "'");
                return sbuf.toString();
            }
            catch (HBqlException e) {
                return "Error in value";
            }
        }

        public boolean isSinlgeRow() {
            return this.getType() == KeyRangeArgs.Type.SINGLE;
        }

        public boolean isAllRows() {
            return this.getType() == KeyRangeArgs.Type.ALL;
        }

        public Get getGet(final WhereArgs whereArgs,
                          final Collection<ColumnAttrib> columnAttribSet) throws HBqlException, IOException {
            final Get get = new Get(this.getLowerAsBytes());
            whereArgs.setGetArgs(get, columnAttribSet);
            return get;
        }

        public Scan getScan(final WhereArgs whereArgs,
                            final Collection<ColumnAttrib> columnAttribSet) throws HBqlException, IOException {
            final Scan scan = new Scan();
            if (!this.isAllRows()) {
                scan.setStartRow(this.getLowerAsBytes());
                if (!this.isLastRange())
                    scan.setStopRow(this.getUpperAsBytes());
            }
            whereArgs.setScanArgs(scan, columnAttribSet);
            return scan;
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
        if (arg1 == null)
            return new Range(arg0, arg1);
        else
            return new Range(KeyRangeArgs.Type.SINGLE, arg0);
    }

    public static Range newLastRange(final GenericValue arg0) {
        return new Range(KeyRangeArgs.Type.LAST, arg0);
    }

    public static Range newAllRange() {
        return new Range();
    }

    public boolean isValid() {
        return this.getRangeList().size() > 0;
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

}

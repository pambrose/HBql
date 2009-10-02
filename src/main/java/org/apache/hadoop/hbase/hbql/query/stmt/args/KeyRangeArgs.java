package org.apache.hadoop.hbase.hbql.query.stmt.args;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.schema.Schema;
import org.apache.hadoop.hbase.hbql.query.util.HUtil;
import org.apache.hadoop.hbase.hbql.query.util.Lists;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 4, 2009
 * Time: 9:53:48 AM
 */
public class KeyRangeArgs {

    private final List<Range> rangeList;

    public enum Type {
        LAST, RANGE
    }

    public static class Range extends SelectArgs {
        private final KeyRangeArgs.Type type;

        public Range(final GenericValue arg0) {
            this(arg0, null, KeyRangeArgs.Type.LAST);
        }

        public Range(final GenericValue arg0, final GenericValue arg1) {
            this(arg0, arg1, KeyRangeArgs.Type.RANGE);
        }

        private Range(final GenericValue arg0, final GenericValue arg1, final KeyRangeArgs.Type type) {
            super(SelectArgs.Type.KEYRANGE, arg0, arg1);
            this.type = type;
        }

        public String getLower() throws HBqlException {
            return (String)this.getGenericValue(0).getValue(null);
        }

        public String getUpper() throws HBqlException {
            return (String)this.getGenericValue(1).getValue(null);
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

        public boolean isStartLastRange() {
            return this.getType() == KeyRangeArgs.Type.LAST;
        }

        public String asString() {
            try {
                final StringBuilder sbuf = new StringBuilder();
                sbuf.append("'" + this.getLower() + "' TO ");
                if (this.isStartLastRange())
                    sbuf.append("LAST");
                else
                    sbuf.append("'" + this.getUpper() + "'");
                return sbuf.toString();
            }
            catch (HBqlException e) {
                return "Error in value";
            }
        }
    }

    public KeyRangeArgs() {
        this.rangeList = Lists.newArrayList();
    }

    public KeyRangeArgs(final List<Range> rangeList) {
        if (rangeList == null)
            this.rangeList = Lists.newArrayList();
        else
            this.rangeList = rangeList;
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

    public void validateTypes(final boolean allowColumns) throws HBqlException {
        for (final Range range : this.getRangeList())
            range.validateTypes(allowColumns);
    }

    public void optimize() throws HBqlException {
        for (final Range range : this.getRangeList())
            range.optimize();
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

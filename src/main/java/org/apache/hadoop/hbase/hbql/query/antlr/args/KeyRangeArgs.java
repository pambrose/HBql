package org.apache.hadoop.hbase.hbql.query.antlr.args;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.schema.HUtil;
import org.apache.hadoop.hbase.hbql.query.util.Lists;

import java.io.IOException;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 4, 2009
 * Time: 9:53:48 AM
 */
public class KeyRangeArgs {

    private final List<Range> rangeList;

    public static class Range {
        private final boolean recordRange;
        private final String lower;
        private final String upper;

        public Range() {
            this(false, null, null);
        }

        public Range(final String lower) {
            this(true, lower, null);
        }

        public Range(final String lower, final String upper) {
            this(true, lower, upper);
        }

        private Range(final boolean recordRange, final String lower, final String upper) {
            this.recordRange = recordRange;
            this.lower = lower;
            this.upper = upper;
        }

        public String getLower() {
            return this.lower;
        }

        public String getUpper() {
            return this.upper;
        }

        public byte[] getLowerAsBytes() throws IOException, HPersistException {
            return HUtil.ser.getStringAsBytes(this.getLower());
        }

        public byte[] getUpperAsBytes() throws IOException, HPersistException {
            return HUtil.ser.getStringAsBytes(this.getUpper());
        }

        public boolean isStartKeyOnly() {
            return this.getUpper() == null;
        }

        public boolean isRecordRange() {
            return this.recordRange;
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

    public List<Range> getRangeList() {
        return this.rangeList;
    }
}

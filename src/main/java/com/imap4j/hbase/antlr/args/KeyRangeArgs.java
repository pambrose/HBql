package com.imap4j.hbase.antlr.args;

import com.google.common.collect.Lists;

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
        private final String lower;
        private final String upper;

        public Range(final String val) {
            this.lower = val;
            this.upper = val;
        }

        public Range(final String lower, final String upper) {
            this.lower = lower;
            this.upper = upper;
        }

        public String getLower() {
            return lower;
        }

        public String getUpper() {
            return upper;
        }
    }

    public KeyRangeArgs(final List<Range> rangeList) {
        if (rangeList == null)
            this.rangeList = Lists.newArrayList();
        else
            this.rangeList = rangeList;
    }

    public List<Range> getRangeList() {
        return rangeList;
    }
}

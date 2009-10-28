package org.apache.expreval.util;

import org.apache.expreval.client.HBqlException;

public class HUtil {

    public static String getZeroPaddedNumber(final long val, final int width) throws HBqlException {

        final String strval = "" + val;
        final int padsize = width - strval.length();
        if (padsize < 0)
            throw new HBqlException("Value " + val + " exceeds width " + width);

        StringBuilder sbuf = new StringBuilder();
        for (int i = 0; i < padsize; i++)
            sbuf.append("0");

        sbuf.append(strval);
        return sbuf.toString();
    }
}

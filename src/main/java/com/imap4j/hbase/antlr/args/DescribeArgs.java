package com.imap4j.hbase.antlr.args;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 24, 2009
 * Time: 10:31:14 PM
 */
public class DescribeArgs implements ExecArgs {

    private final String className;

    public DescribeArgs(final String className) {
        this.className = className;
    }

    public String getClassName() {
        return this.className;
    }
}
package com.imap4j.hbase.antlr;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 24, 2009
 * Time: 10:31:14 PM
 */
public class DescribeArgs implements ExecArgs {

    private final String classname;

    public DescribeArgs(final String classname) {
        this.classname = classname;
    }

    public String getClassname() {
        return this.classname;
    }
}
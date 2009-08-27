package com.imap4j.hbase.antlr.args;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 24, 2009
 * Time: 10:31:14 PM
 */
public class CreateArgs implements ExecArgs {

    private final String classname;

    public CreateArgs(final String classname) {
        this.classname = classname;
    }

    public String getClassname() {
        return this.classname;
    }
}

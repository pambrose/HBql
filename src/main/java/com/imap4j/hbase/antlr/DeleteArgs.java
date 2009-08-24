package com.imap4j.hbase.antlr;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 20, 2009
 * Time: 11:43:49 PM
 */
public class DeleteArgs implements ExecArgs {

    private final String classname;

    public DeleteArgs(final String classname) {
        this.classname = classname;
    }

    public String getClassname() {
        return classname;
    }
}

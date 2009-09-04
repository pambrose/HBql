package com.imap4j.hbase.antlr.args;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 4, 2009
 * Time: 10:29:16 AM
 */
public class WhereArgs {

    private KeyRangeArgs keyRangeArgs = new KeyRangeArgs(null);


    public KeyRangeArgs getKeyRangeArgs() {
        return keyRangeArgs;
    }

    public void setKeyRangeArgs(final KeyRangeArgs keyRangeArgs) {
        this.keyRangeArgs = keyRangeArgs;
    }
}

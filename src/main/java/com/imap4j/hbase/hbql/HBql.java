package com.imap4j.hbase.hbql;

import com.imap4j.hbase.antlr.SetArgs;
import com.imap4j.hbase.antlr.config.HBqlRule;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 21, 2009
 * Time: 1:09:48 PM
 */
public class HBql {

    public static void exec(final String str) throws HBPersistException {
        final SetArgs sa = (SetArgs)HBqlRule.SET.parse(str);

        final String var = sa.getVariable();
        if (var == null)
            throw new HBPersistException("Error in SET command");

        if (var.equals("classpath")) {
            EnvVars.setClasspath(sa.getValue());
            return;
        }

        throw new HBPersistException("Unknown variable: " + var);
    }
}

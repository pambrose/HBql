package org.apache.hadoop.hbase.hbql.query.antlr.args;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 21, 2009
 * Time: 1:08:12 PM
 */
public class SetArgs implements ExecArgs {

    private final String variable, value;

    public SetArgs(final String variable, final String value) {
        this.variable = variable;
        this.value = value;
    }

    public String getVariable() {
        return variable;
    }

    public String getValue() {
        return value;
    }
}

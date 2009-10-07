package org.apache.hadoop.hbase.hbql.query.cmds;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HConnection;
import org.apache.hadoop.hbase.hbql.client.HOutput;
import org.apache.hadoop.hbase.hbql.query.schema.EnvVars;

import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 21, 2009
 * Time: 1:08:12 PM
 */
public class SetCmd implements ConnectionCmd {

    private final String variable, value;

    public SetCmd(final String variable, final String value) {
        this.variable = variable;
        this.value = value;
    }

    public String getVariable() {
        return variable;
    }

    public String getValue() {
        return value;
    }


    public HOutput execute(final HConnection conn) throws HBqlException, IOException {
        final HOutput retval = new HOutput();
        final String var = this.getVariable();

        if (var == null)
            throw new HBqlException("Error in SET command");

        if (var.equalsIgnoreCase("packagepath")) {
            EnvVars.setPackagePath(this.getValue());
            retval.out.println("PackagePath set to " + this.getValue());
            retval.out.flush();
            return retval;
        }

        throw new HBqlException("Unknown variable: " + var);
    }
}

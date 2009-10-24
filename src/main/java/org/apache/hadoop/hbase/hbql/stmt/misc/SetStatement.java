package org.apache.hadoop.hbase.hbql.stmt.misc;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HOutput;
import org.apache.hadoop.hbase.hbql.query.impl.hbase.ConnectionImpl;
import org.apache.hadoop.hbase.hbql.stmt.ConnectionStatement;
import org.apache.hadoop.hbase.hbql.stmt.schema.EnvVars;

import java.io.IOException;

public class SetStatement implements ConnectionStatement {

    private final String variable, value;

    public SetStatement(final String variable, final String value) {
        this.variable = variable;
        this.value = value;
    }

    public String getVariable() {
        return variable;
    }

    public String getValue() {
        return value;
    }


    public HOutput execute(final ConnectionImpl conn) throws HBqlException, IOException {
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

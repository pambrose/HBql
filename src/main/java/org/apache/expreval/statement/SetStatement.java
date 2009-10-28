package org.apache.expreval.statement;

import org.apache.expreval.hbql.impl.ConnectionImpl;
import org.apache.expreval.schema.EnvVars;
import org.apache.hadoop.hbase.contrib.hbql.client.HBqlException;
import org.apache.hadoop.hbase.contrib.hbql.client.HOutput;

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

        final String var = this.getVariable();

        if (var == null)
            throw new HBqlException("Error in SET command");

        if (var.equalsIgnoreCase("packagepath")) {
            EnvVars.setPackagePath(this.getValue());
            return new HOutput("PackagePath set to " + this.getValue());
        }

        throw new HBqlException("Unknown variable: " + var);
    }
}

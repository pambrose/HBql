package org.apache.hadoop.hbase.hbql.query.expr.value.expr;

public enum Operator {
    PLUS("+"),
    MINUS("-"),
    MULT("*"),
    DIV("/"),
    MOD("%"),
    NEGATIVE("-"),

    EQ("=="),
    GT("<"),
    GTEQ(">="),
    LT("<"),
    LTEQ("<="),
    NOTEQ("!="),

    AND("AND"),
    OR("OR");

    final String opStr;

    Operator(final String opStr) {
        this.opStr = opStr;
    }

    public String toString() {
        return this.opStr;
    }
}
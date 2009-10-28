package org.apache.expreval.expr;

import org.apache.expreval.expr.node.BooleanValue;
import org.apache.expreval.expr.node.DateValue;
import org.apache.expreval.expr.node.NumberValue;
import org.apache.expreval.expr.node.StringValue;

public enum ExpressionType {

    BOOLEANCASE(new TypeSignature(BooleanValue.class)),
    STRINGCASE(new TypeSignature(StringValue.class)),
    DATECASE(new TypeSignature(DateValue.class)),
    NUMBERCASE(new TypeSignature(NumberValue.class)),

    BOOLEANCASEWHEN(new TypeSignature(BooleanValue.class, BooleanValue.class, BooleanValue.class)),
    STRINGCASEWHEN(new TypeSignature(StringValue.class, BooleanValue.class, StringValue.class)),
    DATECASEWHEN(new TypeSignature(DateValue.class, BooleanValue.class, DateValue.class)),
    NUMBERCASEWHEN(new TypeSignature(NumberValue.class, BooleanValue.class, NumberValue.class)),

    BOOLEANCASEELSE(new TypeSignature(BooleanValue.class, BooleanValue.class)),
    STRINGCASEELSE(new TypeSignature(StringValue.class, StringValue.class)),
    DATECASEELSE(new TypeSignature(DateValue.class, BooleanValue.class, DateValue.class)),
    NUMBERCASEELSE(new TypeSignature(NumberValue.class, BooleanValue.class, NumberValue.class)),

    BOOLEANIFTHEN(new TypeSignature(BooleanValue.class, BooleanValue.class, BooleanValue.class, BooleanValue.class)),
    STRINGIFTHEN(new TypeSignature(StringValue.class, BooleanValue.class, StringValue.class, StringValue.class)),
    DATEIFTHEN(new TypeSignature(DateValue.class, BooleanValue.class, DateValue.class, DateValue.class)),
    NUMBERIFTHEN(new TypeSignature(NumberValue.class, BooleanValue.class, NumberValue.class, NumberValue.class)),

    STRINGCALCULATION(new TypeSignature(StringValue.class, StringValue.class, StringValue.class)),
    DATECALCULATION(new TypeSignature(DateValue.class, DateValue.class, DateValue.class)),
    NUMBERCALCULATION(new TypeSignature(NumberValue.class, NumberValue.class, NumberValue.class)),

    STRINGBETWEEN(new TypeSignature(BooleanValue.class, StringValue.class, StringValue.class, StringValue.class)),
    DATEBETWEEN(new TypeSignature(BooleanValue.class, DateValue.class, DateValue.class, DateValue.class)),
    NUMBERBETWEEN(new TypeSignature(BooleanValue.class, NumberValue.class, NumberValue.class, NumberValue.class)),

    BOOLEANNULL(new TypeSignature(BooleanValue.class, BooleanValue.class)),
    STRINGNULL(new TypeSignature(BooleanValue.class, StringValue.class)),
    DATENULL(new TypeSignature(BooleanValue.class, DateValue.class)),
    NUMBERNULL(new TypeSignature(BooleanValue.class, NumberValue.class)),

    STRINGPATTERN(new TypeSignature(BooleanValue.class, StringValue.class, StringValue.class)),

    // Args are left unspecified for IN Stmt
    INSTMT(new TypeSignature(BooleanValue.class));

    private final TypeSignature typeSignature;

    ExpressionType(final TypeSignature typeSignature) {
        this.typeSignature = typeSignature;
    }

    public TypeSignature getTypeSignature() {
        return typeSignature;
    }
}

package com.imap4j.hbase.antlr;

import com.imap4j.hbase.hbql.HPersistException;
import com.imap4j.hbase.hbql.expr.node.DateValue;
import com.imap4j.hbase.hbql.expr.node.NumberValue;
import com.imap4j.hbase.hbql.expr.node.ValueExpr;
import com.imap4j.hbase.hbql.expr.value.func.CalcExpr;
import com.imap4j.hbase.hbql.expr.value.func.DateCalcExpr;
import com.imap4j.hbase.hbql.expr.value.func.NumberCalcExpr;
import com.imap4j.hbase.hbql.expr.value.var.DateAttribRef;
import com.imap4j.hbase.hbql.expr.value.var.IntegerAttribRef;
import com.imap4j.hbase.hbql.expr.value.var.LongAttribRef;
import com.imap4j.hbase.hbql.expr.value.var.StringAttribRef;
import com.imap4j.hbase.hbql.schema.ClassSchema;
import com.imap4j.hbase.hbql.schema.FieldType;
import com.imap4j.hbase.hbql.schema.VariableAttrib;
import org.antlr.runtime.BitSet;
import org.antlr.runtime.IntStream;
import org.antlr.runtime.MismatchedTokenException;
import org.antlr.runtime.Parser;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.RecognizerSharedState;
import org.antlr.runtime.TokenStream;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 20, 2009
 * Time: 9:35:18 PM
 */
public class HBaseParser extends Parser {

    private ClassSchema classSchema = null;

    public HBaseParser(final TokenStream input) {
        super(input);
    }

    public HBaseParser(final TokenStream input, final RecognizerSharedState state) {
        super(input, state);
    }

    protected ClassSchema getClassSchema() {
        return this.classSchema;
    }

    protected void setClassSchema(final ClassSchema classSchema) {
        if (classSchema != null)
            this.classSchema = classSchema;
    }

    protected void setClassSchema(final String tablename) throws RecognitionException {
        try {
            final ClassSchema classSchema = ClassSchema.getClassSchema(tablename);
            this.setClassSchema(classSchema);
        }
        catch (HPersistException e) {
            System.out.println("Unknown table: " + tablename);
            throw new RecognitionException(input);
        }
    }

    protected boolean isKeyword(final TokenStream input, final String str) {
        final String s = input.LT(1).getText();
        //System.out.println("Checking for " + str + " and " + s);
        return s != null && s.equalsIgnoreCase(str);
    }

    protected boolean isAttribType(final TokenStream input, final FieldType type) {

        if (this.getClassSchema() == null)
            return false;

        final String s = input.LT(1).getText();
        if (s == null)
            return false;

        final VariableAttrib attrib = this.getClassSchema().getVariableAttribByVariableName(s);
        return attrib != null && attrib.getFieldType() == type;
    }


    protected ValueExpr getValueExpr(final String var) throws RecognitionException {

        if (this.getClassSchema() != null) {

            final VariableAttrib attrib = this.getClassSchema().getVariableAttribByVariableName(var);

            if (attrib != null) {
                switch (attrib.getFieldType()) {
                    case StringType:
                        return new StringAttribRef(var);

                    case LongType:
                        return new LongAttribRef(var);

                    case IntegerType:
                        return new IntegerAttribRef(var);

                    case DateType:
                        return new DateAttribRef(var);
                }
            }
        }

        System.out.println("Unknown variable: " + var);

        throw new RecognitionException(input);
    }

    /*
    public String getErrorMessage(RecognitionException e, String[] tokenNames) {
        List stack = getRuleInvocationStack(e, this.getClass().getName());
        String msg = null;
        if (e instanceof NoViableAltException) {
            NoViableAltException nvae = (NoViableAltException)e;
            msg = " no viable alt; token=" + e.token +
                  " (decision=" + nvae.decisionNumber +
                  " state " + nvae.stateNumber + ")" +
                  " decision=<<" + nvae.grammarDecisionDescription + ">>";
        }
        else {
            msg = super.getErrorMessage(e, tokenNames);
        }
        return stack + " " + msg;
    }

    public String getTokenErrorDisplay(Token t) {
        return t.toString();
    }
    */

    protected void mismatch(IntStream input, int ttype, BitSet follow) throws RecognitionException {
        throw new MismatchedTokenException(ttype, input);
    }

    public Object recoverFromMismatchedSet(IntStream input,
                                           RecognitionException e,
                                           BitSet follow) throws RecognitionException {
        throw e;
    }

    public static boolean attemptRecovery = false;

    protected void handleRecognitionException(final RecognitionException re) throws RecognitionException {

        if (attemptRecovery) {
            reportError(re);
            recover(input, re);
        }
        else {
            throw re;
        }
    }

    public NumberValue getLeftAssociativeNumberValues(final List<NumberValue> exprList, final List<CalcExpr.OP> opList) {

        if (exprList.size() == 1)
            return exprList.get(0);

        NumberValue root = new NumberCalcExpr(exprList.get(0), opList.get(0), exprList.get(1));
        for (int i = 1; i < opList.size(); i++)
            root = new NumberCalcExpr(root, opList.get(i), exprList.get(i + 1));
        return root;
    }

    public DateValue getLeftAssociativeDateValues(final List<DateValue> exprList, final List<CalcExpr.OP> opList) {

        if (exprList.size() == 1)
            return exprList.get(0);

        DateValue root = new DateCalcExpr(exprList.get(0), opList.get(0), exprList.get(1));
        for (int i = 1; i < opList.size(); i++)
            root = new DateCalcExpr(root, opList.get(i), exprList.get(i + 1));
        return root;
    }
}

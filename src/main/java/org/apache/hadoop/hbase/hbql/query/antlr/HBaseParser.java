package org.apache.hadoop.hbase.hbql.query.antlr;

import org.antlr.runtime.BitSet;
import org.antlr.runtime.IntStream;
import org.antlr.runtime.MismatchedTokenException;
import org.antlr.runtime.Parser;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.RecognizerSharedState;
import org.antlr.runtime.TokenStream;
import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;
import org.apache.hadoop.hbase.hbql.query.expr.value.func.Operator;
import org.apache.hadoop.hbase.hbql.query.expr.value.func.ValueCalcExpr;
import org.apache.hadoop.hbase.hbql.query.expr.value.var.BooleanAttribRef;
import org.apache.hadoop.hbase.hbql.query.expr.value.var.DateAttribRef;
import org.apache.hadoop.hbase.hbql.query.expr.value.var.IntegerAttribRef;
import org.apache.hadoop.hbase.hbql.query.expr.value.var.LongAttribRef;
import org.apache.hadoop.hbase.hbql.query.expr.value.var.StringAttribRef;
import org.apache.hadoop.hbase.hbql.query.schema.HBaseSchema;
import org.apache.hadoop.hbase.hbql.query.schema.Schema;
import org.apache.hadoop.hbase.hbql.query.schema.VariableAttrib;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 20, 2009
 * Time: 9:35:18 PM
 */
public class HBaseParser extends Parser {

    private Schema schema = null;

    public HBaseParser(final TokenStream input) {
        super(input);
    }

    public HBaseParser(final TokenStream input, final RecognizerSharedState state) {
        super(input, state);
    }

    protected Schema getSchema() {
        return this.schema;
    }

    protected void setSchema(final Schema schema) {
        if (schema != null)
            this.schema = schema;
    }

    protected Schema setSchema(final String tablename) throws RecognitionException {

        try {
            final HBaseSchema schema = HBaseSchema.findSchema(tablename);
            this.setSchema(schema);
            return schema;
        }
        catch (HPersistException e) {
            System.out.println("Unknown table: " + tablename);
            throw new RecognitionException(input);
        }
    }

    protected boolean isKeyword(final TokenStream input, final String str) {
        final String s = input.LT(1).getText();
        return s != null && s.equalsIgnoreCase(str);
    }

    protected ValueExpr getVariableRef(final String var) throws RecognitionException {

        if (this.getSchema() != null) {

            final VariableAttrib attrib = this.getSchema().getVariableAttribByVariableName(var);

            if (attrib != null) {
                switch (attrib.getFieldType()) {

                    case KeyType:
                    case StringType:
                        return new StringAttribRef(attrib);

                    case LongType:
                        return new LongAttribRef(attrib);

                    case IntegerType:
                        return new IntegerAttribRef(attrib);

                    case DateType:
                        return new DateAttribRef(attrib);

                    case BooleanType:
                        return new BooleanAttribRef(attrib);

                    default:
                        System.out.println("Invalid type: " + attrib.getFieldType().name() + " in getVariableRef()");
                }
            }
            else {
                System.out.println("Invalid variable: " + var + " in getVariableRef()");
            }
        }
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

    public Object recoverFromMismatchedSet(final IntStream input,
                                           final RecognitionException e,
                                           final BitSet follow) throws RecognitionException {
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

    public ValueExpr getLeftAssociativeValueExprs(final List<ValueExpr> exprList, final List<Operator> opList) {

        if (exprList.size() == 1)
            return exprList.get(0);

        ValueExpr root = new ValueCalcExpr(exprList.get(0), opList.get(0), exprList.get(1));
        for (int i = 1; i < opList.size(); i++)
            root = new ValueCalcExpr(root, opList.get(i), exprList.get(i + 1));
        return root;
    }

}

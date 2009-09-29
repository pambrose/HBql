package org.apache.hadoop.hbase.hbql.query.antlr;

import org.antlr.runtime.BitSet;
import org.antlr.runtime.IntStream;
import org.antlr.runtime.MismatchedTokenException;
import org.antlr.runtime.Parser;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.RecognizerSharedState;
import org.antlr.runtime.TokenStream;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.expr.value.func.DelegateCalculation;
import org.apache.hadoop.hbase.hbql.query.expr.value.func.Operator;
import org.apache.hadoop.hbase.hbql.query.expr.value.var.BooleanColumn;
import org.apache.hadoop.hbase.hbql.query.expr.value.var.DateColumn;
import org.apache.hadoop.hbase.hbql.query.expr.value.var.IntegerColumn;
import org.apache.hadoop.hbase.hbql.query.expr.value.var.LongColumn;
import org.apache.hadoop.hbase.hbql.query.expr.value.var.StringColumn;
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
        catch (HBqlException e) {
            System.out.println("Unknown table: " + tablename);
            throw new RecognitionException(input);
        }
    }

    protected boolean isKeyword(final TokenStream input, final String str) {
        final String s = input.LT(1).getText();
        return s != null && s.equalsIgnoreCase(str);
    }

    protected GenericValue getVariable(final String var) throws RecognitionException {

        if (this.getSchema() != null) {

            final VariableAttrib attrib = this.getSchema().getVariableAttribByVariableName(var);

            if (attrib != null) {
                switch (attrib.getFieldType()) {

                    case KeyType:
                    case StringType:
                        return new StringColumn(attrib);

                    case LongType:
                        return new LongColumn(attrib);

                    case IntegerType:
                        return new IntegerColumn(attrib);

                    case DateType:
                        return new DateColumn(attrib);

                    case BooleanType:
                        return new BooleanColumn(attrib);

                    default:
                        System.out.println("Invalid type: " + attrib.getFieldType().name());
                }
            }
            else {
                System.out.println("Invalid variable: " + var);
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

    public GenericValue getLeftAssociativeGenericValues(final List<GenericValue> exprList, final List<Operator> opList) {

        if (exprList.size() == 1)
            return exprList.get(0);

        GenericValue root = new DelegateCalculation(exprList.get(0), opList.get(0), exprList.get(1));
        for (int i = 1; i < opList.size(); i++)
            root = new DelegateCalculation(root, opList.get(i), exprList.get(i + 1));
        return root;
    }

}

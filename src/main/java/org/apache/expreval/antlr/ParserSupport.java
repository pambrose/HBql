package org.apache.expreval.antlr;

import org.antlr.runtime.BitSet;
import org.antlr.runtime.IntStream;
import org.antlr.runtime.MismatchedTokenException;
import org.antlr.runtime.Parser;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.RecognizerSharedState;
import org.antlr.runtime.TokenStream;
import org.apache.expreval.expr.Operator;
import org.apache.expreval.expr.calculation.DelegateCalculation;
import org.apache.expreval.expr.node.GenericValue;
import org.apache.expreval.schema.ColumnDescription;
import org.apache.expreval.schema.DefinedSchema;
import org.apache.hadoop.hbase.contrib.hbql.client.HBqlException;

import java.util.List;

public class ParserSupport extends Parser {

    public ParserSupport(final TokenStream input) {
        super(input);
    }

    public ParserSupport(final TokenStream input, final RecognizerSharedState state) {
        super(input, state);
    }

    protected boolean isKeyword(final TokenStream input, final String str) {
        final String s = input.LT(1).getText();
        return s != null && s.equalsIgnoreCase(str);
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

    public final static boolean attemptRecovery = false;

    protected void handleRecognitionException(final RecognitionException re) throws RecognitionException {

        if (attemptRecovery) {
            reportError(re);
            recover(input, re);
        }
        else {
            throw re;
        }
    }

    public GenericValue getLeftAssociativeGenericValues(final List<GenericValue> exprList,
                                                        final List<Operator> opList) {
        if (exprList.size() == 1)
            return exprList.get(0);

        GenericValue root = new DelegateCalculation(exprList.get(0), opList.get(0), exprList.get(1));

        for (int i = 1; i < opList.size(); i++)
            root = new DelegateCalculation(root, opList.get(i), exprList.get(i + 1));

        return root;
    }

    // This keeps antlr code out of DefinedSchema, which is accessed server-side in HBase
    public static DefinedSchema newDefinedSchema(final TokenStream input,
                                                 final List<ColumnDescription> columList) throws RecognitionException {
        try {
            return new DefinedSchema(columList);
        }
        catch (HBqlException e) {
            e.printStackTrace();
            throw new RecognitionException(input);
        }
    }
}

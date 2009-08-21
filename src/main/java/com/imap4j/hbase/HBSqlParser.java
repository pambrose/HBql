package com.imap4j.hbase;

import org.antlr.runtime.BitSet;
import org.antlr.runtime.IntStream;
import org.antlr.runtime.MismatchedTokenException;
import org.antlr.runtime.Parser;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.RecognizerSharedState;
import org.antlr.runtime.TokenStream;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 20, 2009
 * Time: 9:35:18 PM
 */
public class HBSqlParser extends Parser {

    public HBSqlParser(final TokenStream input) {
        super(input);
    }

    public HBSqlParser(final TokenStream input, final RecognizerSharedState state) {
        super(input, state);
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
}

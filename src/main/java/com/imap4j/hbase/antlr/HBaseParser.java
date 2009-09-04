package com.imap4j.hbase.antlr;

import com.imap4j.hbase.hbql.HPersistException;
import com.imap4j.hbase.hbql.schema.ClassSchema;
import com.imap4j.hbase.hbql.schema.FieldAttrib;
import com.imap4j.hbase.hbql.schema.FieldType;
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

    private boolean isNextTokenOfType(final TokenStream input, final FieldType type) {
        if (this.getClassSchema() == null)
            return false;
        final String s = input.LT(1).getText();
        final FieldAttrib attrib = this.getClassSchema().getFieldAttribMapByVarName().get(s);
        return attrib != null && attrib.getFieldType() == type;
    }

    protected boolean isStringAttrib(final TokenStream input) {
        return this.isNextTokenOfType(input, FieldType.StringType);
    }

    protected boolean isIntAttrib(final TokenStream input) {
        return this.isNextTokenOfType(input, FieldType.IntegerType);
    }

    protected boolean isDateAttrib(final TokenStream input) {
        return this.isNextTokenOfType(input, FieldType.DateType);
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

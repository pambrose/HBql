package org.apache.expreval.client;

import org.antlr.runtime.RecognitionException;

public class ParseException extends HBqlException {
    final RecognitionException recognitionException;

    public ParseException(final RecognitionException recognitionException, final String s) {
        super(s);
        this.recognitionException = recognitionException;
    }

    public RecognitionException getRecognitionException() {
        return recognitionException;
    }
}
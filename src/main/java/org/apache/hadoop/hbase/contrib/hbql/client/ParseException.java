package org.apache.hadoop.hbase.contrib.hbql.client;

import org.antlr.runtime.RecognitionException;

public class ParseException extends HBqlException {
    private final RecognitionException recognitionException;

    public ParseException(final RecognitionException recognitionException, final String s) {
        super(s);
        this.recognitionException = recognitionException;
    }

    public RecognitionException getRecognitionException() {
        return recognitionException;
    }
}
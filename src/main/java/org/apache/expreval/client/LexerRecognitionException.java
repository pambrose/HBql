package org.apache.expreval.client;

import org.antlr.runtime.RecognitionException;

public class LexerRecognitionException extends RuntimeException {

    RecognitionException recognitionExecption;

    public LexerRecognitionException(final RecognitionException recognitionExecption, final String s) {
        super(s);
        this.recognitionExecption = recognitionExecption;
    }

    public RecognitionException getRecognitionExecption() {
        return this.recognitionExecption;
    }
}
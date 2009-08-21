package com.imap4j.hbase.config;

import com.imap4j.imap.antlr.util.GrammarRule;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 20, 2009
 * Time: 9:56:22 PM
 */
public enum Rule {

    QUERY(Grammar.HBSql, "query"),
    NONE(null, null);

    private final GrammarRule grammarRule;

    Rule(final Grammar grammar, final String rule) {
        this.grammarRule = (grammar != null) ? GrammarRule.newInstance(grammar.getGrammarDef(), rule) : null;
    }

    private GrammarRule getGrammarRule() {
        return grammarRule;
    }

    public Object parse(final String str, Object... args) {
        return this.getGrammarRule().parse(str, args);
    }

}

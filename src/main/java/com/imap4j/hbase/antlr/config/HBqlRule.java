package com.imap4j.hbase.antlr.config;

import com.imap4j.imap.antlr.util.GrammarRule;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 20, 2009
 * Time: 9:56:22 PM
 */
public enum HBqlRule {

    QUERY(HBqlGrammar.HBql, "query_stmt"),
    DELETE(HBqlGrammar.HBql, "delete_stmt"),
    SET(HBqlGrammar.HBql, "set_stmt"),
    NONE(null, null);

    private final GrammarRule grammarRule;

    HBqlRule(final HBqlGrammar grammar, final String rule) {
        this.grammarRule = (grammar != null) ? GrammarRule.newInstance(grammar.getGrammarDef(), rule) : null;
    }

    private GrammarRule getGrammarRule() {
        return grammarRule;
    }

    public Object parse(final String str, Object... args) {
        return this.getGrammarRule().parse(str, args);
    }

}

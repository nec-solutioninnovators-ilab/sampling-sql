/*
Sampling-SQL

Copyright (c) 2015-2017 NEC Solution Innovators, Ltd.

This software is released under the MIT License, See the LICENSE file
in the project root for more information.
*/
package com.necsoft.vtc.ssql;

import java.util.LinkedList;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.Vocabulary;
import org.antlr.v4.runtime.tree.ErrorNode;
import org.antlr.v4.runtime.tree.RuleNode;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.antlr.v4.runtime.tree.Tree;

/**
 * Utility class, that provides common operations.
 */
class RewriterCommon {

	/**
	 * Listener class that records error events occurred during parse.
	 */
	static class ErrorListener extends BaseErrorListener {

		LinkedList<String> errors = new LinkedList<>();

		@Override
		public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine, String msg, RecognitionException e) {
			errors.add("syntax error at line " + line + ", column " + (charPositionInLine + 1) + "; " + shortenMessage(msg));
		}

		// Shorten message which is longer than 160 characters.
		private String shortenMessage(String message) {
			if (message != null && message.length() > 160) {
				message = message.substring(0, 160) + "...";
			}
			return message;
		}
	}

	/**
	 * Replace substring that matches literal /&#42;&lt; or literal &gt;&#42;/ with a space character from specified input String.
	 * The literal /&#42;&lt; indicates start of the sampling comment, and the literal &gt;&#42;/ indicates end of the sampling comment.
	 * ( Sampling comment begins with /&#42;&lt; and extends to &gt;&#42;/ )
	 * @param input	String to replace
	 * @return	replaced String
	 */
	static String removeSamplingComment(String input) {
		return input.replace("/*<", " ").replace(">*/", " ");
	}

	// For debug purpose. Returns human readable string which is representing graph of parse tree.
	static String prettyStringTree(Tree tree, Parser parser) {
		if (tree == null || parser == null) {
			throw new IllegalArgumentException("null");
		}
		StringBuilder sb = new StringBuilder();
		Vocabulary vocabulary = parser.getVocabulary();
		String[] ruleNames = parser.getRuleNames();
		buildStringTree(0, sb, tree, vocabulary, ruleNames);
		return sb.toString();
	}

	private static void buildStringTree(final int depth, final StringBuilder sb, final Tree t, final Vocabulary vocaburary, String[] ruleNames) {
		sb.append('\n');

		if (t instanceof RuleNode) {
			indent(sb, "P", depth);
			RuleNode node = (RuleNode)t;
			sb.append(ruleNames[node.getRuleContext().getRuleIndex()]);
		}
		else if (t instanceof ErrorNode) {
			indent(sb, "E", depth);
			ErrorNode node = (ErrorNode)t;
			sb.append(node.toString());
			sb.append(" <-- ***ERROR***");
		}
		else if (t instanceof TerminalNode) {
			indent(sb, "L", depth);
			TerminalNode node = (TerminalNode)t;
			Token token = node.getSymbol();
			sb.append(token.getText());
			int tt = token.getType();
			sb.append(" T:");
			sb.append(vocaburary.getDisplayName(tt));
		}

		if (t.getChildCount() == 0) {
			return;
		}

		for (int i = 0; i < t.getChildCount(); i++) {
			buildStringTree(depth+1, sb, t.getChild(i), vocaburary, ruleNames);
		}
	}

	private static void indent(StringBuilder sb, String header, int depth) {
		sb.append(header);
		int indent = depth * 2 + (4 - header.length());
		for (int i = 0; i < indent; i++) {
			sb.append(' ');
		}
	}

}

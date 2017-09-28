/*
Sampling-SQL

Copyright (c) 2015-2017 NEC Solution Innovators, Ltd.

This software is released under the MIT License, See the LICENSE file
in the project root for more information.
*/
package com.necsoft.vtc.ssql;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ErrorNode;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;

import com.necsoft.vtc.ssql.SamplingSqlParser.ColumnAliasContext;
import com.necsoft.vtc.ssql.SamplingSqlParser.FromItemContext;
import com.necsoft.vtc.ssql.SamplingSqlParser.ParseContext;
import com.necsoft.vtc.ssql.SamplingSqlParser.SampleClauseContext;
import com.necsoft.vtc.ssql.SamplingSqlParser.SampleItemContext;
import com.necsoft.vtc.ssql.SamplingSqlParser.SampleTableContext;
import com.necsoft.vtc.ssql.SamplingSqlParser.SelectStmtContext;
import com.necsoft.vtc.ssql.SamplingSqlParser.TableAliasContext;
import com.necsoft.vtc.ssql.SamplingSqlParser.TableNameContext;
import com.necsoft.vtc.ssql.SamplingSqlParser.TopSelectStmtContext;
import com.necsoft.vtc.ssql.SamplingSqlParser.UntilClauseContext;
import com.necsoft.vtc.ssql.SamplingSqlParser.WithClauseContext;

/**
 * A SQL rewriter for SELECT statement including sampling query syntax.
 * This Class generates SELECT statement that executes UNTIL clause condition of specified sampling level.
 * And generates SELECT statement expanding sampling table to inline view which executes sampling at specified sampling level.
 */
class SamplingSqlRewriter implements SqlRewriter {

	private final DatabaseDef databaseDef;
	private final int base = 2; //4;
	private final int numLevel;
	private final boolean ivMode;

	private final SamplingSqlParser parser;
	private final ParseContext tree;
	private boolean sampling;

	private final RewriterCommon.ErrorListener errorListener;

	private static final String THIS_KEYWORD = "THIS";
	private static final String THIS_MARKER = "/*<THIS>*/";

	private static final String FACTOR_COLUMN_NAME = "_FACTOR";

	/**
	 * Constructor
	 * @param databaseDef	meta information of sampling tables
	 * @param numLevel	number of sampling level
	 * @param input	SELECT statement including sampling syntax
	 * @param rewriteInlineViewEnabled	whether generate inline view or not 
	 */
	SamplingSqlRewriter(DatabaseDef databaseDef, int numLevel, String input, boolean rewriteInlineViewEnabled) {
		this.databaseDef = databaseDef;
		this.numLevel = numLevel;
		this.ivMode = rewriteInlineViewEnabled;

		errorListener = new RewriterCommon.ErrorListener();

		// prepare lexer rule
		CharStream inputs = new ANTLRInputStream(RewriterCommon.removeSamplingComment(input));
		SamplingSqlLexer lexer = new SamplingSqlLexer(inputs);
		lexer.removeErrorListeners();
		lexer.addErrorListener(errorListener);

		// count occurrence of SAMPLE and UNTIL keyword, and record first occurrence of unsupported keywords
		String unsupportedWord = null;
		int sampleKeywordCount = 0;
		int untilKeywordCount = 0;
		for (Token t : lexer.getAllTokens()) {
			int type = t.getType();
			if (type == SamplingSqlLexer.SAMPLE) {
				sampleKeywordCount++;
			}
			else if (type == SamplingSqlLexer.UNTIL) {
				untilKeywordCount++;
			}
			else if (type == SamplingSqlLexer.UNSUPPORTEDWORD) {
				if (unsupportedWord == null) {
					unsupportedWord = t.getText();
				}
			}
		}
		// process based on occurrence of SAMPLE and UNTIL keyword, and unsupported keywords.
		String errormsg = null;
		if (sampleKeywordCount == 0) {
			if (untilKeywordCount == 0) {
				// rewrite unnecessary, the SQL is not applicable to rewriting
				sampling = false;
			}
			else {
				errormsg = "SAMPLE keyword not found";
			}
		}
		else if (untilKeywordCount == 0) {
			errormsg = "UNTIL keyword not found";
		}
		else if (sampleKeywordCount == 1) {
			if (untilKeywordCount == 1) {
				if (unsupportedWord == null) {
					// do rewrite
					sampling = true;
				}
				else {
					errormsg =  "'" + unsupportedWord + "' is unsupported with sampling.";
				}
			}
			else {
				errormsg = "can't use UNTIL keyword more than once";
			}
		}
		else {
			errormsg = "can't use SAMPLE keyword more than once";
		}
		if (errormsg != null) {
			throw new UnsupportedOperationException(errormsg);
		}
		lexer.reset();

		// prepare parser rule
		CommonTokenStream tokens = new CommonTokenStream(lexer);
		parser = new SamplingSqlParser(tokens);
		parser.removeErrorListeners();
		parser.addErrorListener(errorListener);

		// execute parse
		tree = parser.parse();
	}

	@Override
	public boolean hasSampleClause() {
		return tree.topSelectStmt() != null && tree.topSelectStmt().sampleClause() != null;
	}

	@Override
	public QuerySet rewrite(int targetLv) throws SQLException {
		if (!sampling) {
			// rewrite unnecessary
			return null;
		}
		if (targetLv < 0 || targetLv >= numLevel) {
			throw new IllegalArgumentException("targetLv = " + targetLv);
		}
		if (!errorListener.errors.isEmpty()) {
			// throw first error if the ErrorListener records any errors.
			throw new SQLException(errorListener.errors.getFirst());
		}
		try {
			Visitor visitor = new Visitor(base, targetLv);
			return visitor.getQuerySet(tree);
		}
		catch (UnsupportedOperationException e) {
			// throw SQLException, if any errors occurred during visiting the parse tree.
			throw new SQLException(e);
		}
	}

	/** A class to visit parse tree */
	private class Visitor extends SamplingSqlBaseVisitor<String> {

		private final int base;
		private final int targetLevel;
		private final String factorText;

		private String samplingWithQuery = null;
		private String condQuery = null;

		// mapping of inline view, key is table name, and value is inline view text.
		private Map<String, String> ivMap = null;

		public Visitor(int base, int targetLevel) {
			this.base = base;
			this.targetLevel = targetLevel;
			this.factorText = String.valueOf(Math.pow(this.base, this.targetLevel));
		}

		private QuerySet getQuerySet(ParseTree tree) {
			String aggQuery = visit(tree);

			if (ivMode) {
				return new QuerySet(targetLevel, condQuery, aggQuery);
			}

			aggQuery = "WITH " + samplingWithQuery + " " + aggQuery;
			return new QuerySet(targetLevel, condQuery, aggQuery);
		}

		/**
		 * Create WITH clause based on SAMPLE clause. Result string is not include leading WITH keyword.
		 * @param sampleClause	SampleClauseContext
		 * @return	created WITH clause
		 */
		private String makeSamplingWithExpression(SampleClauseContext sampleClause) {
			StringBuilder sb = new StringBuilder();
			for (SampleItemContext sampleItem : sampleClause.sampleItem()) {
				if (sb.length() > 0) {
					sb.append(',');
				}
				String sSampleColumn = visit(sampleItem.sampleColumn());
				for (SampleTableContext sampleTable : sampleItem.sampleTable()) {
					String sSampleTableName = visit(sampleTable.sampleTableName());
					TableDef tableDef = getSampleTableDef(sampleTable, sSampleColumn);
					if (tableDef == null) {
						throw new UnsupportedOperationException(sSampleColumn + " is not sample key of " + sSampleTableName);
					}
					String sLevelColumn = tableDef.getLevelColumnName(sSampleColumn);
					String sSampleTableAlias = sampleTable.sampleTableAlias() == null ? sSampleTableName : visit(sampleTable.sampleTableAlias());
					sb.append(sSampleTableAlias).append(" AS (SELECT * FROM ").append(sSampleTableName)
					.append(" WHERE ").append(sSampleTableName).append('.').append(sLevelColumn).append(" <= ").append(~targetLevel)
					.append(')');
					;
				}
			}
			return sb.toString();
		}

		/**
		 * Create mapping of inline view based on SAMPLE clause, and set to ivMap field.
		 * @param sampleClause	SampleClauseContext
		 */
		private void makeInlineViewMap(SampleClauseContext sampleClause) {
			ivMap = new HashMap<>();
			StringBuilder sb = new StringBuilder();
			for (SampleItemContext sampleItem : sampleClause.sampleItem()) {
				String sSampleColumn = visit(sampleItem.sampleColumn());
				for (SampleTableContext sampleTable : sampleItem.sampleTable()) {
					String sSampleTableName = visit(sampleTable.sampleTableName());
					TableDef tableDef = getSampleTableDef(sampleTable, sSampleColumn);
					if (tableDef == null) {
						throw new UnsupportedOperationException(sSampleColumn + " is not sample key of " + sSampleTableName);
					}
					String sLevelColumn = tableDef.getLevelColumnName(sSampleColumn);
					String sSampleTableAlias = sampleTable.sampleTableAlias() == null ? sSampleTableName : visit(sampleTable.sampleTableAlias());
					sb.setLength(0);
					sb.append("(SELECT * FROM ").append(sSampleTableName)
					.append(" WHERE ").append(sSampleTableName).append('.').append(sLevelColumn).append(" <= ").append(~targetLevel)
					.append(") AS ").append(sSampleTableAlias)
					;
					ivMap.put(SqlUtils.unquoteIdentifier(sSampleTableAlias), sb.toString());
				}
			}
		}

		/**
		 * Returns TableDef object that is corresponding to specified Sample Table and Sample Column (or Unit Key) pair.
		 * Returns null if Sample Table and Sample Column (or Unit Key) pair is not found in the DatabaseDef.
		 * @param sampleTable	SampleTableContext
		 * @param sampleColumnName	name text of SampleColumn
		 * @return	TableDef object
		 */
		private TableDef getSampleTableDef(SampleTableContext sampleTable, String sampleColumnName) {
			List<TerminalNode> ids = sampleTable.sampleTableName().qualifiedName().IDENTIFIER();
			String localTableName = visit(ids.get(ids.size() - 1));
			TableDef tableDef = databaseDef.get(localTableName);
			if (tableDef != null && tableDef.isSampleColumn(sampleColumnName)) {
				return tableDef;
			}
			return null;
		}

		/**
		 * Search ParserRuleContext from specified context towards the parent direction.
		 * @param ctx	starting context to search
		 * @param ruleIndex	rule index to search
		 * @return	ParserRuleContext object, or null if specified rule index is not found.
		 */
		private ParserRuleContext findAncestor(ParserRuleContext ctx, int ruleIndex) {
			while (ctx != null && ctx.getRuleIndex() != ruleIndex) {
				ctx = ctx.getParent();
			}
			return ctx;
		}

		/**
		 * Iterate specified list, and returns concatenated text of each list's element.
		 * @param list	list to iterate
		 * @param sep	separator character of concatenating
		 * @return	concatenated text
		 */
		private String visit(List<? extends ParseTree> list, char sep) {
			StringBuilder sb = new StringBuilder();
			for (ParseTree tree : list) {
				if (sb.length() > 0) {
					sb.append(sep);
				}
				String result = visit(tree);
				if (result != null) {
					sb.append(result);
				}
			}
			return sb.toString();
		}

		// Override methods of SamplingSqlBaseVisitor below

		@Override
		public String visitTopSelectStmt(TopSelectStmtContext ctx) {
			SampleClauseContext sampleClause = ctx.sampleClause();
			WithClauseContext withClause = ctx.withClause();
			UntilClauseContext untilClause = ctx.untilClause();
			if (ivMode) {
				makeInlineViewMap(sampleClause);//makeInClauseInlineViewMap(sampleClause);
				if (withClause != null) {
					condQuery = visit(withClause);
				}
				if (untilClause != null) {
					String sUntilContent = visit(untilClause.content(), ' ');
					if (sUntilContent.contains(THIS_MARKER)) {
						throw new UnsupportedOperationException("can't use THIS in the top UNTIL clause");
					}
					if (condQuery != null && condQuery.length() > 0) {
						condQuery += " SELECT " + sUntilContent;
					}
					else {
						condQuery = "SELECT " + sUntilContent;
					}
				}
				return super.visitTopSelectStmt(ctx);
			}
			//if (sampleClause != null) {
				samplingWithQuery = makeSamplingWithExpression(sampleClause);
			//}
			if (withClause != null) {
				// concatenate WITH clause generated from SAMPLE clause and top level WITH clause.
				String sWithContent = visit(withClause.content(), ' ');
				if (sWithContent != null) {
					samplingWithQuery = samplingWithQuery + "," + sWithContent;
				}
			}
			if (untilClause != null) {
				String sUntilContent = visit(untilClause.content(), ' ');
				if (sUntilContent.contains(THIS_MARKER)) {
					throw new UnsupportedOperationException("can't use THIS in the top UNTIL clause");
				}
				else {
					condQuery = "WITH " + samplingWithQuery + " SELECT " + sUntilContent;
				}
			}
			return super.visitTopSelectStmt(ctx);
		}

		@Override
		public String visitSelectStmt(SelectStmtContext ctx) {
			UntilClauseContext untilClause = ctx.untilClause();
			if (untilClause != null) {
				// when current processing SELECT statement contains UNTIL clause, generate condition query

				String sUntilContent = visit(untilClause.content(), ' ');
				if (sUntilContent.contains(THIS_MARKER)) {
					// if UNTIL clause contains THIS, generate subquery based on SELECT statement referenced by THIS, and replace THIS by the subquery
					if (findAncestor(ctx, SamplingSqlParser.RULE_untilClause) == null) {
						// SELECT clause after UNTIL clause is not parse for replacing THIS
						StringBuilder sb = new StringBuilder();
						sb.append("(SELECT *");
						if (ctx.fromClause() != null) {
							sb.append(' ').append(visit(ctx.fromClause()));
						}
						if (ctx.whereClause() != null) {
							sb.append(' ').append(visit(ctx.whereClause()));
						}
						sb.append(") AS ").append(THIS_KEYWORD);
						String subQuery = sb.toString();
						sUntilContent = sUntilContent.replace(THIS_MARKER, subQuery);
					}
				}
				else {
					// if UNTIL clause not contains THIS, replacing unnecessary.
				}

				if (ivMode) {
					// generate condition query using inline view
					if (condQuery != null && condQuery.length() > 0) {
						condQuery += " SELECT " + sUntilContent;
					}
					else {
						condQuery = "SELECT " + sUntilContent;
					}
				}
				else {
					// generate condition query using WITH clause
					condQuery = "WITH " + samplingWithQuery + " SELECT " + sUntilContent;
				}
			}
			return super.visitSelectStmt(ctx);
		}

		@Override
		public String visitSampleClause(SampleClauseContext ctx) {
			//return super.visitSampleClause(ctx);
			// do nothing because SAMPLE clause is processed by other methods
			return null;
		}

		@Override
		public String visitWithClause(WithClauseContext ctx) {
			if (!ivMode && ctx.getParent() instanceof TopSelectStmtContext) {
				// do nothing because top level WITH clause is processed by other methods
				return null;
			}
			return super.visitWithClause(ctx);
		}

		@Override
		public String visitUntilClause(UntilClauseContext ctx) {
			//return super.visitUntilClause(ctx);
			// do nothing because UNTIL clause is processed by other methods
			return null;
		}

		@Override
		public String visitFromItem(FromItemContext ctx) {
			if (ivMode && ivMap != null) {
				// rewrite tables that are corresponding to sampling inline view.
				// but, only rewrite tables that are children of FromItem.
				TableNameContext tableName = ctx.tableName();
				if (tableName != null) {
					// FromItem directly has TableNameContext instance
					String tableNameText = visit(tableName);
					String ivText = ivMap.get(SqlUtils.unquoteIdentifier(tableNameText));
					if (ivText != null) {
						// when table name is replacing target of inline view
						TableAliasContext tableAlias = ctx.tableAlias();
						if (tableAlias != null) {
							// when TableAlias is used.
							// replace alias name of inline view to original TableAlias.
							StringBuilder sb = new StringBuilder(ivText);
							int ivAliasPosition = sb.lastIndexOf(") AS ") + 5;
							sb.setLength(ivAliasPosition);
							String tableAliasText = visit(tableAlias);
							sb.append(tableAliasText);
							List<ColumnAliasContext> columnAliasList = ctx.columnAlias();
							if (columnAliasList != null && !columnAliasList.isEmpty()) {
								// tableName AS tableAlias (columnAlias,...)
								String columnAliasText = visit(columnAliasList, ',');
								sb.append('(').append(columnAliasText).append(')');
							}
							return sb.toString();
						}
						else {
							// returns inline view when TableAlias is not used.
							return ivText;
						}
					}
				}
			}
			return super.visitFromItem(ctx);
		}

		// Override methods of AbstarctParseTreeVisitor below

		@Override
		public String visitTerminal(TerminalNode node) {
			int tokenType = node.getSymbol().getType();
			if (tokenType == Token.EOF) {
				return null;
			}

			String text = node.getText();
			if (text == null) {
				return null;
			}

			if (tokenType == SamplingSqlParser.IDENTIFIER) {
				if (SqlUtils.isQuotedIdentifier(text)) {
					// quoted identifiers are case-sensitive.
				}
				else {
					// otherwise, case-insensitive, convert to lower case.
					text = text.toLowerCase();
				}
			}

			String uppertext = text.toUpperCase();
			if (THIS_KEYWORD.equals(uppertext)) {
				// replace literal "THIS" to marker.
				return THIS_MARKER;
			}
			if (FACTOR_COLUMN_NAME.equals(uppertext)) {
				// replace literal "_FACTOR" to number of multiplier at processing level.
				return factorText;
			}
			if (";".equals(text)) {
				// discard literal ";"
				return null;
			}
			return text;
		}

		@Override
		public String visitErrorNode(ErrorNode node) {
			throw new UnsupportedOperationException("parse error: " + node.getText());
		}

		@Override
		protected String aggregateResult(String aggregate, String nextResult) {
			if (aggregate == null) {
				return nextResult;
			}
			else if (nextResult == null) {
				return aggregate;
			}
			else {
				return aggregate + ' ' + nextResult;
			}
		}

	}

}

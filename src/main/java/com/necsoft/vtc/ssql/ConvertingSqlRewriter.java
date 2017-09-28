/*
Sampling-SQL

Copyright (c) 2015-2017 NEC Solution Innovators, Ltd.

This software is released under the MIT License, See the LICENSE file
in the project root for more information.
*/
package com.necsoft.vtc.ssql;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ErrorNode;
import org.antlr.v4.runtime.tree.TerminalNode;
import com.necsoft.vtc.ssql.ConvertingSqlParser.ColumnDefContext;
import com.necsoft.vtc.ssql.ConvertingSqlParser.ColumnNameContext;
import com.necsoft.vtc.ssql.ConvertingSqlParser.ColumnOptContext;
import com.necsoft.vtc.ssql.ConvertingSqlParser.CreateTableStatementContext;
import com.necsoft.vtc.ssql.ConvertingSqlParser.DataTypeContext;
import com.necsoft.vtc.ssql.ConvertingSqlParser.ParseContext;
import com.necsoft.vtc.ssql.ConvertingSqlParser.SampleColumnContext;
import com.necsoft.vtc.ssql.ConvertingSqlParser.SampleItemContext;
import com.necsoft.vtc.ssql.ConvertingSqlParser.SampleTableNameContext;
import com.necsoft.vtc.ssql.ConvertingSqlParser.SamplekeyClauseContext;
import com.necsoft.vtc.ssql.ConvertingSqlParser.SortkeyClauseContext;
import com.necsoft.vtc.ssql.ConvertingSqlParser.TableBodyContext;
import com.necsoft.vtc.ssql.ConvertingSqlParser.TableNameContext;

/**
 * A SQL rewriter for CREATE TABLE statement including sampling syntax.
 * This Class generates CREATE TABLE statement for sampling table that is added LEVEL columns to control sorting.
 * And generates INSERT statement transferring rows of origin table to sampling table.
 * And generates CREATE TABLE and INSERT statement that stores meta information of sampling tables.
 */
class ConvertingSqlRewriter {

	// hash function parameters
	// hash(x) = ( A x + B ) % P
	private static int[] HASH_A = {1341504661,665956287,431491107,1335278997,396946527,703897305,1984561111,362176395,655741751,1081711543,1405927759,1734660673,1099383871,1178756025,39446973,1966157063,15636249,669480169,84255615,403639351,1143125353,1298593229,115852241,1783001137,963248377,437802077,567329307,152750621,630117869,619928111,694476233,1866921441,973781549,263097097,801317809,1864450487,135488279,1061903937,1996216591,110482725,1887426355,305533105,121602725,2112365967,117285921,310001753,1571174427,1521872841,898039841,815548043,1351999187,921825107,1381788659,2097125217,2058595281,1507079751,251799607,294398823,1976238797,898834071,946841867,1854445907,1487138231,883965755,614338951,1700834799,2078110943,424266815,1003134603,1663333813,501771397,1942804203,1636532393,1973554943,1075443763,1379319689,1643684825,1646734557,1384131999,2028126547,1350750579,1159399649,589572609,2106624271,91822789,455049343,1612413723,406971273,1509722157,2025604159,1725687413,113467623,1854458725,1596589107,2118420527,1523162139,451662827,1554494773,105807449,942821021,1511792093,1022521251,1430579009,855114905,1999826067,860426145,66506099,1613760907,884756519,1095097311,1193325049,133243355,1501652449,206693649,606631363,265791803,1615509265,1264853619,1108556199,1135356637,473618855,1524061083,1916437941,517152483,1510006791,1249305641,666288619,696399355,215359045,993020067,598139107,997036469,615743613,1596212205,792624589,569624527,542667473,774757317,1384024005,1593764201,251745159,842173155,252751329,1103145875,745466261,1093498397,1463201521,471283729,2107446893,1652444095,1063230737,248511635,1829534839,886537333,1813693603,1716417671,539705417,90871683,777949745,2106957333,1999765395,290737453,1143633901,1592793165,747279327,1339964475,372972239,1153400207,331939219,529277433,1349077875,1416922183,1991147997,1645661129,2008718301,807948191,392099797,645705937,593552119,2142046373,771707559,1968476205,171462835,604994147,151544161,1562783033,1445429625,565986757,1026364835,193932323,368908363,446813279,2144103743,906583825,1406410687,393333959,685819643,1522867859,1211992271,2120897467,625066741,959985027,1184413685,1423582581,702179713,1710183271,1539349839,1953894005,1505378309,1668169969,757133567,866901655,389119949,1946818527,1091432165,416767975,691879575,1738243371,1174245819,2078484525,1752604797,423219051,151879025,1060588905,301444487,1987143803,266103135,1839496229,704690413,787028283,1628967127,1327745763,813191659,1052682371,1976763633,348795011,908292281,379867379,5129531,277790951,1885533967,186433109,1045080499,1369919213,1688462981,1639455369,644044451,1005543411,2146511417,303377175,561630793,768978933,1075937045,1802175841,2001365607};
	private static int[] HASH_B = {305151913,1506487997,1220166885,2038725529,1493588191,882708833,502659797,1753355701,2089272317,1318825407,1938536723,645606305,989707687,1115072591,828768127,255017641,1031488429,1616384115,37399941,654299111,1791296391,1200516397,1068419449,806265485,281425215,5012537,1690371387,1691939763,1898703933,2023773437,1545462115,1473891437,469889727,199361307,1541570047,1642715185,692492863,1136969099,1336193203,1434203261,2093568295,1404312895,774335231,942267543,1941694517,956233111,2005733023,1780375043,2083780351,810422717,741642387,1072086357,239577411,811596489,924667777,1803078625,429817381,2073334687,159997053,1922748875,1853010643,1968284669,1366012467,1909033341,356690467,877153277,1338742717,1281155797,2074617381,254207927,1627130695,2066721413,1603453817,1575878553,767468449,186112941,1959079389,1721822709,1613498061,1352245171,2130941145,1132139875,902872021,1449265819,388143509,2120879447,1708696773,1957326657,226165267,1825650277,1017514035,544075587,495481013,1784715093,1396309169,654696457,852907691,1674370791,1981129015,2050000505,671913693,1824317241,180471051,1066099387,1119924849,188295207,191589155,88783329,640247761,1829764465,1420037759,899569281,639807223,1598645021,469823391,2119065095,694643313,98347021,247268571,1385069571,680378797,875708555,411896689,901635367,1788428745,1873785499,49318987,21285065,939097941,1175189669,2078687537,1264074387,343083477,1036501611,1614007137,306616139,556782627,645081645,2044787767,382309665,268393241,1595275357,2124847155,724311687,976730521,60460489,422288659,2065623877,1445853427,576986589,373029849,1260658149,732595329,1724154887,1006851203,324098519,1103502731,768425059,1622165743,357753863,311650033,1430057257,1386632517,1169921221,553000941,667337281,825422335,1073478491,1580153521,1278560789,1324849009,870289903,1523688147,176965717,1964907291,2057108921,1797125409,1130722407,111802731,1110049115,1772164847,1542729223,635210333,2053872145,1770591479,1616874267,692488133,1392913887,2107945903,1605735279,236790011,1029779025,720605627,1099998111,596363231,216238235,694862945,622527209,84041225,605899437,450910221,2029426077,2145409593,1281666819,1469156365,531670091,437174599,890945083,893685999,884351261,140275243,1410011673,633730985,1119353031,797735879,1785547051,653884741,1806503659,1378159793,1928444665,516675475,1480666215,1937459295,1023137715,1637541653,1470468299,478751255,838167039,1162169065,584342719,311893947,1398148749,908970647,690289015,1249960863,1281897297,1565376007,617383393,1942580161,159744107,1297776801,1383320665,1847365213,787394851,672329317,592385189,2084622711,52550557,494388021,1839786467,1081634495,1258637473,1605155357,555014629,2103173303};
	private static int HASH_P = 2147483647; // 2^31 - 1

	private DBType dbtype;
	private RewriterCommon.ErrorListener errorListener;
	private ConvertingSqlLexer lexer;
	private CommonTokenStream tokens;
	private ConvertingSqlParser parser;
	private ParseContext tree;

	private String baseTableName;
	private String samplingTableName;
	private String stagingTableName;
	private String clusterIndexName;
	private String sortkeylistExpr;

	private List<String> keyNames = new ArrayList<>();
	private List<String[]> keyExprs = new ArrayList<>();
	private List<String> hashNames = new ArrayList<>();
	private List<String> levelNames = new ArrayList<>();
	private List<String> columns = new ArrayList<>();
	private List<String> dataTypes = new ArrayList<>();
	private List<String> baseSortkeyNames = new ArrayList<>();

	/**
	 * Constructor
	 * @param input	CREATE TABLE statement including sampling syntax
	 * @param dbType	underlying database
	 */
	ConvertingSqlRewriter(String input, DBType dbType) {

		this.dbtype = dbType;

		errorListener = new RewriterCommon.ErrorListener();

		CharStream inputs = new ANTLRInputStream(RewriterCommon.removeSamplingComment(input));
		lexer = new ConvertingSqlLexer(inputs);
		lexer.removeErrorListeners();
		lexer.addErrorListener(errorListener);

		tokens = new CommonTokenStream(lexer);
		parser = new ConvertingSqlParser(tokens);
		parser.removeErrorListeners();
		parser.addErrorListener(errorListener);

		tree = parser.parse();
	}

	String getErrorMessage() {
		if (errorListener.errors.isEmpty()) {
			return null;
		}
		return errorListener.errors.getFirst();
	}

	boolean hasSampleClause() {
		return tree.samplekeyClause() != null;
	}

	// Visitor for inspecting and retrieving information of sampling table
	private class InspectingVisitor extends ConvertingSqlBaseVisitor<Void> {

		private void visit() {
			visit(tree);
		}

		// Override methods of ConvertingSqlBaseVisitor below

		@Override
		public Void visitSampleTableName(SampleTableNameContext ctx) {
			samplingTableName = ctx.getText();
			return super.visitSampleTableName(ctx);
		}

		@Override
		public Void visitSampleItem(SampleItemContext ctx) {
			SampleColumnContext sampleColumnCtx = ctx.sampleColumn();
			keyNames.add(sampleColumnCtx.getText());
			List<ColumnNameContext> columnNameCtxList = ctx.columnName();
			int numColumnName = columnNameCtxList.size();
			if (numColumnName == 0) {
				keyExprs.add(new String[] {sampleColumnCtx.getText()});
			}
			else {
				String[] keyExpr = new String[columnNameCtxList.size()];
				for (int i = 0; i < columnNameCtxList.size(); i++) {
					ColumnNameContext columnNameCtx = columnNameCtxList.get(i);
					keyExpr[i] = columnNameCtx.getText();
				}
				keyExprs.add(keyExpr);
			}
			return super.visitSampleItem(ctx);
		}

		@Override
		public Void visitTableName(TableNameContext ctx) {
			String tableName = ctx.getText();
			baseTableName = tableName;
			return super.visitTableName(ctx);
		}

		@Override
		public Void visitColumnDef(ColumnDefContext ctx) {
			ColumnNameContext columnNameCtx = ctx.columnName();
			String columnName = columnNameCtx.getText();
			columns.add(columnName);
			DataTypeContext dataTypeCtx = ctx.dataType();
			String dataType = dataTypeCtx.getText();
			dataTypes.add(dataType);
			for (ColumnOptContext columnOptCtx : ctx.columnOpt()) {
				if (columnOptCtx.SORTKEY() != null) {
					baseSortkeyNames.add(columnName);
					break;
				}
			}
			return super.visitColumnDef(ctx);
		}

		@Override
		public Void visitSortkeyClause(SortkeyClauseContext ctx) {
			for (ColumnNameContext columnNameCtx : ctx.columnName()) {
				baseSortkeyNames.add(columnNameCtx.getText());
			}
			return super.visitSortkeyClause(ctx);
		}

	}

	// Visitor for generating CREATE TABLE statement.
	private class CreateTableVisitor extends ConvertingSqlBaseVisitor<String> {

		private String visit() {
			return visit(tree);
		}

		// Override methods of ConvertingSqlBaseVisitor below

		@Override
		public String visitSamplekeyClause(SamplekeyClauseContext ctx) {
			//return super.visitSamplekeyClause(ctx);
			// remove SAMPLEKEY clause from generating CREATE TABLE statement.
			return null;
		}

		@Override
		public String visitCreateTableStatement(CreateTableStatementContext ctx) {
			//return super.visitCreateTableStatement(ctx);

			String createTable = super.visitCreateTableStatement(ctx);
			if (dbtype == DBType.AR) {
				// generate SORTKEY clause
				StringBuilder sb = new StringBuilder();
				sb.append(createTable);
				sb.append("\nSORTKEY (");
				sb.append(sortkeylistExpr);
				sb.append(')');
				return sb.toString();
			}

			return createTable;
		}

		@Override
		public String visitTableName(TableNameContext ctx) {
			//return super.visitTableName(ctx);
			// replace table name
			return " " + stagingTableName;
		}

		@Override
		public String visitTableBody(TableBodyContext ctx) {
			//return super.visitTableBody(ctx);
			StringBuilder sb = new StringBuilder();
			if (levelNames.size() > 1) {
				sb.append("\n_LEVEL_SUM SMALLINT,");
			}
			for (String levelName : levelNames) {
				sb.append('\n');
				sb.append(levelName);
				sb.append(" SMALLINT,");
			}
			sb.append(super.visitTableBody(ctx));
			return sb.toString();
		}

		@Override
		public String visitColumnOpt(ColumnOptContext ctx) {
			//return super.visitColumnOpt(ctx);
			// SORTKEY clause is generated in other method,
			// thus remove SORTKEY clause from generating CREATE TABLE statement.
			if (ctx.SORTKEY() != null) {
				return null;
			}
			return super.visitColumnOpt(ctx);
		}

		@Override
		public String visitSortkeyClause(SortkeyClauseContext ctx) {
			// SORTKEY clause is generated in other method,
			// thus remove SORTKEY clause from generating CREATE TABLE statement.
			return null;
		}

		// Override methods AbstarctParseTreeVisitor below

		@Override
		public String visitTerminal(TerminalNode node) {
			String text;
			Token token = node.getSymbol();
			int tokenType = token.getType();
			if (tokenType == Token.EOF) {
				text = "";
			}
			else {
				text = node.getText();
			}

			if (text == null) {
				return null;
			}

			// regenerate comments and spaces in original SQL
			StringBuilder sb = new StringBuilder();
			List<Token> hiddenTokens = tokens.getHiddenTokensToLeft(token.getTokenIndex());
			if (hiddenTokens != null) {
				for (Token t : hiddenTokens) {
					//sb.append('<');
					sb.append(t.getText());
					//sb.append('>');
				}
			}
			sb.append(text);
			return sb.toString();
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
				return aggregate + nextResult;
			}
		}

	}

	void prepare() {
		// retrieve information from original CREATE TABLE statement.
		(new InspectingVisitor()).visit();

		stagingTableName = samplingTableName;
		clusterIndexName = "cidx_" + stagingTableName;
		for (String samplekey : keyNames) {
			hashNames.add("_" + samplekey + "_HASH");
			levelNames.add("_" + samplekey + "_LEVEL");
		}

		// for SORTKEY or CLUSTER INDEX:
		// create comma separated text from level sum column and all level columns.
		StringBuilder sb = new StringBuilder();
		if (levelNames.size() == 1) {
			sb.append(levelNames.get(0));
		}
		else {
			sb.append("_LEVEL_SUM");
			for (String levelName : levelNames) {
				sb.append(',');
				sb.append(levelName);
			}
		}
		for (String baseSortkey : baseSortkeyNames) {
			sb.append(',');
			sb.append(baseSortkey);
		}
		sortkeylistExpr = sb.toString();
	}

	String getCreateTableSQL() {
		CreateTableVisitor createTableVisitor = new CreateTableVisitor();
		return createTableVisitor.visit();
	}

	String getCreateMetaTableSQL() {
		return "CREATE TABLE _SAMPLE_KEY_DEFS (\n" +
				" TABLE_NAME VARCHAR(250),\n" +
				" KEY_NAME VARCHAR(250),\n" +
				" KEY_ORDER INTEGER,\n" +
				" LEVEL_NAME VARCHAR(250),\n" +
				" BASE_EXPR VARCHAR(250),\n" +
				" BASE_TYPE VARCHAR(250),\n" +
				" PRIMARY KEY (TABLE_NAME,KEY_ORDER)\n" +
				")\n";
	}

	private String findColumnDataType(String findName) {
		if (findName == null) {
			for (int i = 0; i < columns.size(); i++) {
				if (columns.get(i) == null) {
					return dataTypes.get(i);
				}
			}
			return null;
		}
		String findStr = SqlUtils.isQuotedIdentifier(findName) ? SqlUtils.unquoteIdentifier(findName) : SqlUtils.toLowerCase(findName);
		for (int i = 0; i < columns.size(); i++) {
			String columnName = columns.get(i);
			String colStr = SqlUtils.isQuotedIdentifier(columnName) ? SqlUtils.unquoteIdentifier(columnName) : SqlUtils.toLowerCase(columnName);
			if (findStr.equals(colStr)) {
				return dataTypes.get(i);
			}
		}
		return null;
	}

	String getInsertMetaTableSQL() {
		int numSamplekeys = keyNames.size();

		StringBuilder sb = new StringBuilder();
		sb.append("INSERT INTO _SAMPLE_KEY_DEFS VALUES\n");
		for (int i = 0; i < numSamplekeys; i++) {
			if (i > 0) {
				sb.append(",\n");
			}
			sb.append('(');
			sb.append('\'');
			sb.append(samplingTableName);
			sb.append('\'');
			sb.append(',');
			sb.append('\'');
			sb.append(keyNames.get(i));
			sb.append('\'');
			sb.append(',');
			sb.append(i+1);
			sb.append(',');
			sb.append('\'');
			sb.append(levelNames.get(i));
			sb.append('\'');
			sb.append(',');
			sb.append('\'');
			String[] keyExpr = keyExprs.get(i);
			for (int j = 0; j < keyExpr.length; j++) {
				if (j > 0) {
					sb.append(',');
				}
				sb.append(keyExpr[j]);
			}
			sb.append('\'');
			sb.append(',');
			sb.append('\'');
			for (int j = 0; j < keyExpr.length; j++) {
				if (j > 0) {
					sb.append(',');
				}
				sb.append(findColumnDataType(keyExpr[j]));
			}
			sb.append('\'');
			sb.append(')');
		}

		return sb.toString();
	}

	String getInsertTableSQL() {
		int numSamplekeys = keyNames.size();
		int numColumns = columns.size();

		StringBuilder sb = new StringBuilder();
		sb.append("INSERT INTO ");
		sb.append(stagingTableName);
		sb.append('\n');
		sb.append(" SELECT\n");

		if (numSamplekeys > 1) {
			sb.append(' ');
			for (int j = 0; j < numSamplekeys; j++) {
				if (j > 0) {
					sb.append('+');
				}
				sb.append(levelNames.get(j));
			}
			sb.append(" AS _LEVEL_SUM,\n");
		}

		for (int j = 0; j < numSamplekeys; j++) {
			sb.append(' ');
			sb.append(levelNames.get(j));
			sb.append(",\n");
		}
		for (int j = 0; j < numColumns; j++) {
			sb.append(' ');
			sb.append(columns.get(j));
			if (j < numColumns - 1) {
				sb.append(',');
			}
			sb.append('\n');
		}

		sb.append(" FROM (\n");
		sb.append("  SELECT\n");
		for (int j = 0; j < numSamplekeys; j++) {
			sb.append("  ");
			buildLevelExpr(sb, hashNames.get(j), levelNames.get(j));
			sb.append(",\n");
		}
		for (int j = 0; j < numColumns; j++) {
			sb.append("  ");
			sb.append(columns.get(j));
			if (j < numColumns - 1) {
				sb.append(',');
			}
			sb.append('\n');
		}

		sb.append("  FROM (\n");
		sb.append("   SELECT\n");
		for (int j = 0; j < numSamplekeys; j++) {
			sb.append("   ");
			buildHashExpr(sb, keyExprs.get(j), hashNames.get(j));
			sb.append(",\n");
		}
		for (int j = 0; j < numColumns; j++) {
			sb.append("   ");
			sb.append(columns.get(j));
			if (j < numColumns - 1) {
				sb.append(',');
			}
			sb.append('\n');
		}

		sb.append("   FROM ").append(baseTableName).append('\n');
		sb.append("  ) AS _HASH_INLINE\n");
		sb.append(" ) AS _LEVEL_INLINE\n");

		return sb.toString();
	}

	List<String> getClusteringSQL() {
		switch (dbtype) {
		case PG:
			return createClusterPostgreSQL();
		default:
			return Collections.emptyList();
		}
	}

	private void buildLevelExpr(StringBuilder sb, String hashkey, String levelkey) {
		sb.append("CASE WHEN ").append(hashkey).append("=0 THEN -32 ELSE -31 + CAST(")
		.append("FLOOR(LOG(").append(hashkey).append(")/CAST(0.301029995663981 AS DOUBLE PRECISION))")
		.append(" AS SMALLINT) END AS ").append(levelkey);
	}

	private void buildHashExpr(StringBuilder sb, String[] samplekeys, String hashkey) {
		if (samplekeys.length == 1) {
			sb.append("CAST(((")
			.append(HASH_A[0]).append(" * (").append(samplekeys[0]).append(" & CAST(4294967295 AS BIGINT))")
			.append(" + ").append(HASH_B[0])
			.append(") % ").append(HASH_P).append(") AS INTEGER) AS ").append(hashkey);
		}
		else {
			sb.append('(');
			for (int j = 0; j < samplekeys.length; j++) {
				if (j > 0) {
					sb.append(" # "); // bitwise XOR
				}
				sb.append("CAST(((")
				.append(HASH_A[j]).append(" * (").append(samplekeys[j]).append(" & CAST(4294967295 AS BIGINT))")
				.append(" + ").append(HASH_B[j])
				.append(") % ").append(HASH_P).append(") AS INTEGER)");
			}
			sb.append(") AS ").append(hashkey);
		}
	}

	private List<String> createClusterPostgreSQL() {
		List<String> sqls = new ArrayList<>();
		StringBuilder sb = new StringBuilder();

		// on PostgreSQL
		// create composite index on level sum column and all level columns.
		// cluster sampling table using the composite index.
		// create index on each level columns.
		sb.setLength(0);
		sb.append("CREATE INDEX ").append(clusterIndexName)
		.append(" ON ").append(stagingTableName).append('(')
		.append(sortkeylistExpr)
		.append(')');
		sqls.add(sb.toString());

		sb.setLength(0);
		sb.append("CLUSTER ").append(stagingTableName).append(" USING ").append(clusterIndexName);
		sqls.add(sb.toString());

		if (levelNames.size() > 1) {
			for (String levelName : levelNames) {
				sb.setLength(0);
				sb.append("CREATE INDEX ").append("lidx").append(levelName)
				.append(" ON ").append(stagingTableName).append('(')
				.append(levelName)
				.append(')');
				sqls.add(sb.toString());
			}
		}

		return sqls;
	}

}

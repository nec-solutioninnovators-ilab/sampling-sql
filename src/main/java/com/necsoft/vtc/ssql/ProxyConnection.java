/*
Sampling-SQL

Copyright (c) 2015-2017 NEC Solution Innovators, Ltd.

This software is released under the MIT License, See the LICENSE file
in the project root for more information.
*/
package com.necsoft.vtc.ssql;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A class that wraps {@code java.sql.Connection} of underlying JDBC driver.
 */
public class ProxyConnection extends AbstractWrapperConnection {

	/** field storing meta information of sampling tables. */
	private DatabaseDef databaseDef = new DatabaseDef();
	/** number of levels that is used in iterative sampling. The default value is 32. */
	private int numLevel = 32;

	/** true if sampling enabled. */
	private boolean samplingEnabled = true;
	/** true if generate inline view query when sampling enabled. */
	private boolean rewriteInlineViewEnabled = false;
	/** field to identify the underlying database. */
	private DBType dbType = DBType.PG;

	/**
	 * Creates an instance by assigning the argument {@code conn} to the field {@code super.conn} .
	 * @param conn	Connection instance of underlying database
	 */
	ProxyConnection(Connection conn) {
		super(conn);
	}

	/**
	 * For experimental purpose. Change number of level that is used in iterative sampling.
	 * @param numLevel	number of level
	 */
	void setNumLevel(int numLevel) {
		this.numLevel = numLevel;
	}

	/**
	 * Returns true if sampling is enabled, otherwise false.
	 * @return	true if sampling is enabled, otherwise false
	 */
	public boolean isSamplingEnabled() {
		return samplingEnabled;
	}

	/**
	 * Enable/disable sampling process
	 * @param samplingEnabled	true to enable sampling, false to disable
	 */
	public void setSamplingEnabled(boolean samplingEnabled) {
		this.samplingEnabled = samplingEnabled;
	}

	/**
	 * Returns true, if generate inline view query, otherwise false.
	 * @return
	 */
	public boolean isRewriteInlineViewEnabled() {
		return rewriteInlineViewEnabled;
	}

	/**
	 * Enable/disable generating inline view query
	 * @param rewriteInlineViewEnabled	true to enable generating inline view query, false to disable
	 */
	public void setRewriteInlineViewEnabled(boolean rewriteInlineViewEnabled) {
		this.rewriteInlineViewEnabled = rewriteInlineViewEnabled;
	}

	/**
	 * Set rewriting behavior that depends on the underlying database.
	 * @param dbType	constants of DBType
	 */
	void setDBType(DBType dbType) {
		this.dbType = dbType;
	}

	@Override
	public Statement createStatement() throws SQLException {
		return new StatementImpl(conn.createStatement());
	}

	@Override
	public Statement createStatement(int resultSetType, int resultSetConcurrency)
			throws SQLException {
		return new StatementImpl(conn.createStatement(resultSetType, resultSetConcurrency));
	}

	@Override
	public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		return new StatementImpl(conn.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability));
	}

	/**
	 * A class that wraps {@code java.sql.Statement} of underlying JDBC driver.
	 */
	private class StatementImpl extends AbstractWrapperStatement implements ProxyStatement {

		private StatementImpl(Statement srcStmt) throws SQLException {
			super(srcStmt);
		}

		// Create an instance of SqlRewriter
		private SqlRewriter newRewriter(String sql) throws SQLException {
			try {
				return new SamplingSqlRewriter(databaseDef, numLevel, sql, rewriteInlineViewEnabled);
			}
			catch (UnsupportedOperationException e) {
				throw new SQLException(e);
			}
		}

		// Create an instance of ConvertingRewriter
		private ConvertingSqlRewriter newConvertingRewriter(String sql) throws SQLException {
			try {
				return new ConvertingSqlRewriter(sql, dbType);
			}
			catch (UnsupportedOperationException e) {
				throw new SQLException(e);
			}
		}

		@Override
		public boolean execute(String sql) throws SQLException {
			// throws SQLFeatureNotSupportedException if specified argument contains multiple SQL statements.
			mustBeSingleQuery(sql);

			// first, assume that SQL is SELECT statement.
			SqlRewriter rewriter = null;
			SQLException exception = null;
			try {
				rewriter = newRewriter(sql);
			}
			catch (SQLException e) {
				exception = e;
			}
			if (exception == null) {
				if (!samplingEnabled || !hasSampleClause(rewriter)) {
					return stmt.execute(sql);
				}
				return execute(rewriter);
			}

			// second, assume that SQL is CREATE TABLE statement.
			ConvertingSqlRewriter convertingRewriter = null;
			try {
				convertingRewriter = newConvertingRewriter(sql);
			}
			catch (SQLException e) {
				exception = e;
			}
			if (convertingRewriter != null) {
				if (convertingRewriter.hasSampleClause()) {
					String msg = convertingRewriter.getErrorMessage();
					if (msg != null) {
						throw new SQLException(msg);
					}
					convertTable(convertingRewriter);
					return false;
				}
			}

			// finally, delegate to underlying database
			return stmt.execute(sql);
		}

		/**
		 * Throws SQLFeatureNotSupportedException if specified argument contains multiple SQL statements.
		 * @param sql	SQL to test
		 * @throws SQLException	if specified argument contains multiple SQL statements
		 */
		private void mustBeSingleQuery(String sql) throws SQLException {
			Pattern p = Pattern.compile("^[^;]*;?\\s*");
			Matcher m = p.matcher(sql);
			if (!m.find()) {
				throw new SQLFeatureNotSupportedException("Multiple queries are not supported. sql:" + sql);
			}
		}

		@Override
		public ResultSet executeQuery(String sql) throws SQLException {
			SqlRewriter rewriter = newRewriter(sql);

			if (!samplingEnabled || !hasSampleClause(rewriter)) {
				return stmt.executeQuery(sql);
			}
			else {
				return executeSamplingQuery(rewriter);
			}
		}

		/** Returns true if specified rewriter is generated from SQL including SAMPLE clause. */
		private boolean hasSampleClause(SqlRewriter rewriter) throws SQLException {
			return rewriter.hasSampleClause();
		}

		/**
		 * Executes condition query by decreasing target level until condition is satisfied, then executes aggregate query.
		 * @param rewriter	rewriter
		 * @return	a ResultSet object that contains the data produced by the aggregate query
		 * @throws SQLException	if error occurs, or if condition is not satisfied.
		 */
		private ResultSet executeSamplingQuery(SqlRewriter rewriter) throws SQLException {
			String aggSql = doSampling(rewriter);
			ResultSet rs = stmt.executeQuery(aggSql);
			return rs;
		}

		/**
		 * Executes condition query by decreasing target level until condition is satisfied, then executes aggregate query.
		 * @param rewriter	rewriter
		 * @return	true if aggregate query result is a ResultSet object; false if it is an update count or there are no results
		 * @throws SQLException	if error occurs, or if condition is not satisfied.
		 */
		private boolean execute(SqlRewriter rewriter) throws SQLException {
			String aggSql = doSampling(rewriter);
			return stmt.execute(aggSql);
		}

		/**
		 * Generates and executes condition query by decreasing target level until condition is satisfied, then returns aggregate query.
		 * @param rewriter	rewriter
		 * @return	aggregate query when condition query is satisfied.
		 * @throws SQLException	if error occurs, or if condition is not satisfied.
		 */
		private String doSampling(SqlRewriter rewriter) throws SQLException {

			String aggSql = null;

			for (int targetLv = numLevel - 1; targetLv >= 0; targetLv--) {
				databaseDef.load(conn);
				QuerySet querySet = rewriter.rewrite(targetLv);

				if (querySet.condSql == null) {
					aggSql = querySet.aggSql;
					assert aggSql != null;
					return aggSql;
				}

				// execute condition SQL
				boolean checkResult = checkCondition(querySet.condSql);

				// when result of condition SQL is true, ends iteration.
				if (checkResult == true) {
					aggSql = querySet.aggSql;
					break;
				}
			}

			if (aggSql == null) {
				// throws SQLException if condition SQL is not satisfied.
				throw new SQLException("Sample table did not satisfy UNTIL condition.");
			}

			return aggSql;
		}

		/**
		 * Executes condition sql and returns the result.
		 * @param condSql	condition sql
		 * @return	result
		 * @throws SQLException	if error occurs, or condition sql returns result other than boolean type, or returns no result, or returns multiple result.
		 */
		private boolean checkCondition(String condSql) throws SQLException {
			boolean result;
			try (ResultSet rs = stmt.executeQuery(condSql);) {
				if (!rs.next()) {
					throw new SQLException("DB returned empty rows for condition query.");
				}
				try {
					result = rs.getBoolean(1);
				}
				catch (SQLException e) {
					throw new SQLException("Result is not boolean for condition query.", e);
				}
				if (rs.next()) {
					throw new SQLException("DB returned multiple rows for condition query. maybe, UNTIL clause is wrong.");
				}
			}

			return result;
		}

		@Override
		public Connection getConnection() throws SQLException {
			return ProxyConnection.this;
		}

		// Transfer original table to sampling table
		private void convertTable(ConvertingSqlRewriter convertingRewriter) throws SQLException {
			String sql;
			convertingRewriter.prepare();

			sql = convertingRewriter.getCreateTableSQL();
			stmt.execute(sql);

			sql = convertingRewriter.getCreateMetaTableSQL();
			boolean autoCommit = stmt.getConnection().getAutoCommit();
			if (!autoCommit && dbType == DBType.PG) {
				Savepoint sp = null;
				try {
					sp = stmt.getConnection().setSavepoint();
					stmt.execute(sql);
				}
				catch (SQLException ex) {
					String sqlState = ex.getSQLState();
					if (sqlState != null && sqlState.startsWith("42")) {
						// ignore error if meta table already exists
						stmt.getConnection().rollback(sp);
					}
					else {
						throw ex;
					}
				}
				finally {
					try {
						if (sp != null) {
							stmt.getConnection().releaseSavepoint(sp);
						}
					}
					catch (SQLException ignore) {
					}
				}
			}
			else if (!autoCommit && dbType == DBType.AR) {
				// savepoint not supported.
				try {
					// commit transaction for executed DDL that creates sampling table.
					stmt.getConnection().commit();
					stmt.execute(sql);
				}
				catch (SQLException ex) {
					String sqlState = ex.getSQLState();
					if (sqlState != null && sqlState.startsWith("42")) {
						// ignore error if meta table already exists.
						// but end transaction to execute following DML.
						stmt.getConnection().commit();
					}
					else {
						throw ex;
					}
				}
			}
			else {
				try {
					stmt.execute(sql);
				}
				catch (SQLException ex) {
					String sqlState = ex.getSQLState();
					if (sqlState != null && sqlState.startsWith("42")) {
						// ignore error if meta table already exists
					}
					else {
						throw ex;
					}
				}
			}

			sql = convertingRewriter.getInsertMetaTableSQL();
			stmt.execute(sql);

			sql = convertingRewriter.getInsertTableSQL();
			stmt.execute(sql);

			for (String clusteringsql : convertingRewriter.getClusteringSQL()) {
				stmt.execute(clusteringsql);
			}
		}

		@Override
		public String[] getRewrittenQuery(String sql) throws SQLException {
			// first, assume that SQL is SELECT statement.
			SqlRewriter queryRewriter = null;
			try {
				queryRewriter = newRewriter(sql);
			}
			catch (SQLException e) {
				// ignore
			}
			if (queryRewriter != null) {
				if (!samplingEnabled || !hasSampleClause(queryRewriter)) {
					return null;
				}
				int targetLv = 4;
				databaseDef.load(conn);
				QuerySet querySet = queryRewriter.rewrite(targetLv);
				if (querySet != null) {
					return new String[] {querySet.condSql, querySet.aggSql};
				}
				else {
					return null;
				}
			}

			// second, assume that SQL is CREATE TABLE statement.
			ConvertingSqlRewriter convertingRewriter = null;
			try {
				convertingRewriter = newConvertingRewriter(sql);
			}
			catch (SQLException e) {
				//ignore
			}
			if (convertingRewriter != null) {
				if (convertingRewriter.hasSampleClause()) {
					String msg = convertingRewriter.getErrorMessage();
					if (msg != null) {
						throw new SQLException(msg);
					}
					ArrayList<String> sqls = new ArrayList<>();
					convertingRewriter.prepare();

					sql = convertingRewriter.getCreateTableSQL();
					sqls.add(sql);

					sql = convertingRewriter.getCreateMetaTableSQL();
					sqls.add(sql);

					sql = convertingRewriter.getInsertMetaTableSQL();
					sqls.add(sql);

					sql = convertingRewriter.getInsertTableSQL();
					sqls.add(sql);

					for (String clusteringsql : convertingRewriter.getClusteringSQL()) {
						sqls.add(clusteringsql);
					}

					return sqls.toArray(new String[0]);
				}
			}

			return null;
		}
	}

}

/** For debug purpose. It will be obsoleted. */
interface ProxyStatement {
	/**
	 * For debug purpose. Retrieve rewritten SQL.
	 * <p>If the SQL is not re-writable, returns null.
	 * <p>If the SQL is re-writable SELECT statement, returns array of string, 1st element is condition SQL, 2nd element is aggregation SQL.
	 * <p>If the SQL is re-writable CREATE TABLE statement, returns array of string, 1st element is statement to create sampling table, 2nd element is statement to create meta table, 3rd element is statement to insert meta informations, 4th element is statement to transfer original table 5th element is statement to cluster the sampling table, and so on.
	 * @param sql	Original SQL.
	 * @return	Array of String.
	 * @throws SQLException	If error occurs.
	 */
	public String[] getRewrittenQuery(String sql) throws SQLException;
}

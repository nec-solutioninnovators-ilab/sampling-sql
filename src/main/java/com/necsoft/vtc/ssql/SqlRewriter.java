/*
Sampling-SQL

Copyright (c) 2015-2017 NEC Solution Innovators, Ltd.

This software is released under the MIT License, See the LICENSE file
in the project root for more information.
*/
package com.necsoft.vtc.ssql;

import java.sql.SQLException;

/**
 * A container class that stores sampling level, condition query and aggregation query that corresponding to the sampling level
 */
class QuerySet {
	final int level;
	final String condSql;
	final String aggSql;

	QuerySet(int level, String condQuery, String aggQuery) {
		this.level = level;
		this.condSql = condQuery;
		this.aggSql = aggQuery;
	}

}

/**
 * An interface of rewriting SQL select statement.
 */
public interface SqlRewriter {
	/**
	 * Returns condition query and aggregation query that corresponding to the specified level.
	 * @param targetLv	sampling level
	 * @return	condition query and aggregation query that corresponding to the specified level.
	 * Or null, if this SqlRewriter is constructed from plain SQL which not contains SAMPLE clause.
	 * @throws SQLException	if error occurs
	 */
	QuerySet rewrite(int targetLv) throws SQLException;

	/**
	 * Returns true if this SqlRewriter is constructed from SQL which contains SAMPLE clause, otherwise false.
	 * @return	true if this SqlRewriter is constructed from SQL which contains SAMPLE clause, otherwise false
	 */
	boolean hasSampleClause();
}


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
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Class for storing meta information of sampling tables.
 */
public class DatabaseDef {

	/** SELECT statement retrieving information about sampling tables */
	private static final String LOAD_META_SQL = "SELECT TABLE_NAME,KEY_NAME,LEVEL_NAME FROM _SAMPLE_KEY_DEFS ORDER BY KEY_ORDER";

	private Map<String, TableDef> databaseDef;

	/**
	 * Returns TableDef which name matches specified identifier. The identifier may be quoted identifier.
	 * @param id	identifier to find
	 * @return	TableDef, or null if TableDef which name matches the specified identifier is not found.
	 */
	TableDef get(String id) {
		return SqlUtils.findIdentifier(databaseDef, id);
	}

	/**
	 * Load DatabaseDef object that contains meta information about the sampling tables.
	 * This is including information about the sampling table names, unit key names and its ordinal number, level column names corresponding to unit key.
	 * If DatabaseDef already has been loaded, this method do nothing.
	 * @param conn	connection to underlying database
	 * @throws SQLException	if error occurred
	 */
	void load(Connection conn) throws SQLException {
		if (databaseDef != null) {
			return;
		}
		Map<String, TableDef> dbDef = new HashMap<>();
		try (Statement stmt = conn.createStatement();) {
			try (ResultSet rs = stmt.executeQuery(LOAD_META_SQL);) {
				while (rs.next()) {
					String tname = rs.getString(1).toLowerCase();
					String kname = rs.getString(2).toLowerCase();
					TableDef tblDef = dbDef.get(tname);
					if (tblDef == null) {
						tblDef = new TableDef(tname, new ArrayList<ColumnDef>());
						dbDef.put(tname, tblDef);
					}
					tblDef.columnDefs.add(new ColumnDef(kname, true));
					tblDef.sampleColumnDefs.add(new ColumnDef(kname, true));
				}
				databaseDef = dbDef;
			}
		}
	}

}

/** Container class representing columns of sampling table */
class ColumnDef {
	final String columnName;
	final boolean isSampleColumn;

	ColumnDef(String columnName, boolean isSampleColumn) {
		this.columnName = columnName;
		this.isSampleColumn = isSampleColumn;
	}
}

/** Class for representing sampling table */
class TableDef {
	final String tableName;
	final List<ColumnDef> columnDefs;
	final List<ColumnDef> sampleColumnDefs;

	TableDef(String tableName, List<ColumnDef> columnDefs) {
		this.tableName = tableName;
		this.columnDefs = columnDefs;
		sampleColumnDefs = new ArrayList<ColumnDef>();
		for (ColumnDef columnDef : columnDefs) {
			if (columnDef.isSampleColumn) {
				sampleColumnDefs.add(columnDef);
			}
		}
	}

	/**
	 * Returns list of unit keys in this TableDef.
	 * @return	list of unit keys in this TableDef
	 */
	public List<ColumnDef> getSampleColumns() {
		return sampleColumnDefs;
	}

	/**
	 * Tests if specified column name is a unit key in this TableDef.
	 * @param columnName	column name to test
	 * @return	test result
	 */
	boolean isSampleColumn(String columnName) {
		for (ColumnDef columnDef : sampleColumnDefs) {
			if (SqlUtils.unquoteIdentifier(columnDef.columnName).equals(SqlUtils.unquoteIdentifier(columnName))) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Returns number of unit keys in this TableDef.
	 * @return	number of unit keys in this TableDef
	 */
	int getSampleColumnCount() {
		return sampleColumnDefs.size();
	}

	/**
	 * Returns ordinal number of unit keys which matches specified column name in this TableDef.
	 * @param columnName	column name, it may be quoted identifier.
	 * @return	ordinal number of unit keys, or -1 if specified column name is not unit key.
	 */
	int getSampleColumnOrder(String columnName) {
		for (int i = 0; i < sampleColumnDefs.size(); i++) {
			if (SqlUtils.unquoteIdentifier(sampleColumnDefs.get(i).columnName).equals(SqlUtils.unquoteIdentifier(columnName))) {
				return i;
			}
		}
		return -1;
	}

	/**
	 * Returns level column name corresponding to specified ordinal number of unit keys in this TableDef.
	 * @param order	ordinal number of unit keys. Number of first unit key is 0, and next is 1, ...
	 * @return	level column name corresponding to specified ordinal number
	 */
	String getLevelColumnName(int order) {
		String sampleColumnName = sampleColumnDefs.get(order).columnName;
		return makeLevelColumnName(sampleColumnName);
	}

	/**
	 * Returns level column name corresponding to specified unit key name.
	 * @param sampleColumnName	column name of unit key
	 * @return	level column name corresponding to specified unit key name
	 */
	String getLevelColumnName(String sampleColumnName) {
		return makeLevelColumnName(sampleColumnName);
	}

	/**
	 * Returns level column name that is string inserted "_" before the specified string and inserted "_level" after the specified string.
	 * @param sampleColumnName	unit key name
	 * @return	level column name
	 */
	private String makeLevelColumnName(String sampleColumnName) {
		if (SqlUtils.isQuotedIdentifier(sampleColumnName)) {
			return "\"_" + sampleColumnName.substring(1, sampleColumnName.length() - 1) + "_level\"";
		}
		else {
			return "_" + sampleColumnName + "_level";
		}
	}

}

class SqlUtils {
	// private constructor to avoid instantiation.
	private SqlUtils() {};

	/**
	 * Returns a String converted to lower case. This is equivalent to calling {@code s.toLowerCase()}. Or returns null if specified String is null.
	 * @param s	String to convert
	 * @return a String converted to lower case.
	 */
	static String toLowerCase(String s) {
		return s == null ? null : s.toLowerCase();
	}

	/**
	 * Returns non quoted identifier corresponding to the specified string.
	 * If the specified string is a quoted identifier, remove leading and trailing double-quote character ("),
	 * and replace two consecutive double-quotes ("") to single double-quote (").
	 * Or returns specified string if the string is not a quoted identifier.
	 * @param id	identifier string
	 * @return	non quoted identifier
	 */
	static String unquoteIdentifier(String id) {
		if (isQuotedIdentifier(id)) {
			return id.substring(1, id.length() - 1).replace("\"\"", "\"");
		}
		else {
			return id;
		}
	}

	/**
	 * Tests if specified string is quoted identifier. A quoted identifier is enclosed in double-quote character (").
	 * Always returns false if specified string is null or length of the string smaller than 2.
	 * @param s	string to test
	 * @return	true if specified string is quoted identifier, otherwise false.
	 */
	static boolean isQuotedIdentifier(String s) {
		return s != null && s.length() >= 2 && s.charAt(0) == '"' && s.charAt(s.length() - 1) == '"';
	}

	/**
	 * Returns the value to which the specified identifier is mapped, by considering quoted identifier.
	 * @param map	Map to find
	 * @param id	identifier to find.
	 * @return	the value to which the specified id is mapped, or null if specified map contains no mapping for the identifier.
	 */
	static <T> T findIdentifier(Map<String, T> map, String id) {
		String unquotedId = unquoteIdentifier(id);
		T t = map.get(unquotedId);
		if (t != null) {
			return t;
		}
		for (Entry<String, T> entry : map.entrySet()) {
			if (unquoteIdentifier(entry.getKey()).equals(unquotedId)) {
				return entry.getValue();
			}
		}
		String unquotedLowerId = toLowerCase(unquotedId);
		for (Entry<String, T> entry : map.entrySet()) {
			if (toLowerCase(unquoteIdentifier(entry.getKey())).equals(unquotedLowerId)) {
				return entry.getValue();
			}
		}
		return null;
	}
}

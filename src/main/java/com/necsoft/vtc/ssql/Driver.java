/*
Sampling-SQL

Copyright (c) 2015-2017 NEC Solution Innovators, Ltd.

This software is released under the MIT License, See the LICENSE file
in the project root for more information.
*/
package com.necsoft.vtc.ssql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * Defines the constants that are used to identify underlying database.
 */
enum DBType {
	AR,
	PG,
	Unknown
}

/**
 * The Sampling JDBC Driver class.
 */
public class Driver implements java.sql.Driver {

	/** The prefix string that inserted before JDBC URL string of the underlying database */
	private static final String prefix = "sample:";

	// class initializer
	static {
		try {
			// register this Driver
			java.sql.DriverManager.registerDriver(new Driver());
		}
		catch (SQLException e) {
			throw new ExceptionInInitializerError(e);
		}

		// load JDBC Driver class of the underlying database
		try {
			Class.forName("org.postgresql.Driver");
		}
		catch (ClassNotFoundException e) {
			// ignore
		}
		try {
			Class.forName("com.amazon.redshift.jdbc.Driver");
		}
		catch (ClassNotFoundException e) {
			// ignore
		}
	}

	/**
	 * Removes "sample:" prefix in the specified URL.
	 * @param url	URL
	 * @return prefix removed URL
	 */
	private String getSourceUrl(String url) {
		return url.replaceFirst("^" + prefix, "");
	}

	/**
	 * Removes "sample:" prefix in given URL,
	 * and calls {@link java.sql.Driver#connect(String, Properties)} of underlying JDBC Driver,
	 * and returns ProxyConnection that wraps underlying connection.
	 * If given URL does not begins with sampling prefix, returns null.
	 * <p>{@inheritDoc}
	 */
	@Override
	public Connection connect(String url, Properties info) throws SQLException {
		if (!acceptsURL(url)) {
			return null;
		}

		// remove prefix
		String srcUrl = getSourceUrl(url);

		// connect the underlying database
		Connection srcConnection = DriverManager.getConnection(srcUrl, info);

		// create ProxyConnection that wraps the underlying database
		ProxyConnection proxyConnection = new ProxyConnection(srcConnection);

		// decide the underlying database according to the JDBC URL
		if (srcUrl.startsWith("jdbc:")) {
			String subUrl = srcUrl.substring(5);

			if (subUrl.startsWith("postgresql:")) {
				// PostgreSQL
				proxyConnection.setRewriteInlineViewEnabled(true);
				proxyConnection.setDBType(DBType.PG);
			}
			else if (subUrl.startsWith("redshift:")) {
				proxyConnection.setDBType(DBType.AR);
			}
			else {
				proxyConnection.setDBType(DBType.Unknown);
			}
		}
		else {
			proxyConnection.setDBType(DBType.Unknown);
		}

		return proxyConnection;
	}

	/**
	 * Returns true if given URL begins with sampling prefix string. Sampling prefix string is "sample:".
	 * <p>{@inheritDoc}
	 */
	@Override
	public boolean acceptsURL(String url) throws SQLException {
		return url.startsWith(prefix);
	}

	/**
	 * Calls underlying {@link java.sql.Driver#getPropertyInfo(String, Properties)} and returns the result. If given URL does not begins with sampling prefix, returns null.
	 * <p>{@inheritDoc}
	 */
	@Override
	public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
		if (!acceptsURL(url)) {
			return null;
		}

		String srcUrl = getSourceUrl(url);

		return DriverManager.getDriver(srcUrl).getPropertyInfo(srcUrl, info);
	}

	/**
	 * Returns {@code 1}.
	 * <p>{@inheritDoc}
	 */
	@Override
	public int getMajorVersion() {
		return 1;
	}

	/**
	 * Returns {@code 0}.
	 * <p>{@inheritDoc}
	 */
	@Override
	public int getMinorVersion() {
		return 0;
	}

	/**
	 * Returns false.
	 * <p>{@inheritDoc}
	 */
	@Override
	public boolean jdbcCompliant() {
		return false;
	}

	/**
	 * Throws SQLFeatureNotSupportedException.
	 * <p>{@inheritDoc}
	 */
	@Override
	public Logger getParentLogger() throws SQLFeatureNotSupportedException {
		// the SQLState "0A000" indicates "FEATURE NOT SUPPORTED"
		throw new SQLFeatureNotSupportedException("not implemented.", "0A000");
	}

}

/*
    Copyright (c) 2011, 
    Benjamin Adrian <benjamin.horak@gmail.com>
    German Research Center for Artificial Intelligence (DFKI) <info@dfki.de>
    
    All rights reserved.

    This file is part of SCOOBIE.

    SCOOBIE is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    SCOOBIE is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with SCOOBIE.  If not, see <http://www.gnu.org/licenses/>.
 */

package de.dfki.km.perspecting.obie.connection;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import au.com.bytecode.opencsv.CSVReader;

/**
 * 
 * @author adrian
 * @author Andreas Lauer
 */
public class HsqlKB extends PostgresKB {

	private static final Logger log = Logger.getLogger(PostgresKB.class
			.getName());

	public HsqlKB(Connection connection, String session, URI uri)
			throws Exception {
		super(connection, session, uri, "hsql/dbscheme.sql",
				"hsql/indexscheme.sql");
	}

	private String createImportStmtFragment(int rowCount) {
		String fragment = "(";
		for (int i = 0; i < rowCount; i++) {
			if (i != 0) {
				fragment += ",";
			}
			fragment += "?";
		}
		fragment += ")";

		return (fragment);
	}

	@Override
	protected void uploadBulk(File file, String table, String session,
			Connection conn) throws Exception {

		PreparedStatement rowImport = null;
		try {

			FileInputStream fis = new FileInputStream(file);
			InputStreamReader isr = new InputStreamReader(fis, "utf-8");
			CSVReader reader = new CSVReader(isr, ',', '\"');
			String[] nextLine = null;
			while ((nextLine = reader.readNext()) != null) {
				if (rowImport == null) {
					String cmd = "INSERT INTO " + table + " VALUES "
							+ createImportStmtFragment(nextLine.length);
					rowImport = conn.prepareStatement(cmd);
				}

				for (int i = 0; i < nextLine.length; i++) {
					rowImport.setObject(i + 1, nextLine[i]);
				}
				rowImport.addBatch();
			}

			if (rowImport != null)
				rowImport.executeBatch();
		} catch (Exception e) {
			log.log(Level.SEVERE, "Failed to upload CSV file:", e);
		} finally {
			if (rowImport != null)
				rowImport.close();
		}

	}

	@Override
	public ResultSetCursor dbSort(List<CharSequence> index, int maxLength)
			throws Exception {

		String prefix = "SELECT * as string FROM ( VALUES ";
		String suffix = " ) AS t(string) order by string";

		StringBuilder b = new StringBuilder();
		for (int i = 0; i < index.size(); i++) {
			b.append("(CAST(? AS VARCHAR(" + maxLength + ")))");
			b.append(",");
		}
		String query;
		if (b.length() > 0) {
			query = prefix + b.substring(0, b.length() - 1) + suffix;
		} else {
			return null;
		}
		try {
			PreparedStatement pstmt = connection.prepareStatement(query);

			for (int i = 0; i < index.size(); i++) {
				int min = Math.min(maxLength, index.get(i).length());
				pstmt.setString(i + 1, (String) index.get(i)
						.subSequence(0, min));
			}

			ResultSet rs = executeQuery(pstmt, query);
			return new ResultSetCursor(rs);
		} finally {
			if (connection != null)
				connection.close();
		}

	}

	@Override
	protected void createIndexes() throws Exception {

		try {

			InputStream in = PostgresKB.class
					.getResourceAsStream(INDEXSCHEME_SQL);
			BufferedReader reader = new BufferedReader(
					new InputStreamReader(in));

			StringBuilder builder = new StringBuilder();
			for (String line = reader.readLine(); line != null; line = reader
					.readLine()) {
				builder.append(line);
				builder.append("\n");
			}
			reader.close();
			in.close();
			String sqlBatch = builder.toString();
			
			for (String sql : sqlBatch.split(";\n")) {
				Statement s = connection.createStatement();
				int batches = s.executeUpdate(sql);
				s.close();
			}
			
			log.info("Created indexes: " + connection.getCatalog());
		} catch (SQLException e) {
			log.log(Level.SEVERE, PostgresKB.class.getName(), e);
			throw new Exception(e);
		}
	}

	@Override
	protected void createDatabase() throws Exception {

		InputStream in = PostgresKB.class.getResourceAsStream(DBSCHEME_SQL);
		BufferedReader reader = new BufferedReader(new InputStreamReader(in));
		StringBuilder builder = new StringBuilder();
		for (String line = reader.readLine(); line != null; line = reader
				.readLine()) {
			builder.append(line);
			builder.append("\n");
		}
		reader.close();
		in.close();
		String sqlBatch = builder.toString();
		try {

			for (String sql : sqlBatch.split(";\n")) {
				Statement s = connection.createStatement();
				int batches = s.executeUpdate(sql);
				s.close();
			}
			connection.commit();
			log.info("Created scheme: " + connection.getCatalog());
		} catch (SQLException e) {
			printSQLException(e);
			log.log(Level.SEVERE, PostgresKB.class.getName(), e);
			throw new Exception(printSQLException(e));
		}
	}

}

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

package de.dfki.km.perspecting.obie.transducer.model;

import gnu.trove.TIntDoubleHashMap;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import de.dfki.km.perspecting.obie.connection.KnowledgeBase;
import de.dfki.km.perspecting.obie.model.Document;
import de.dfki.km.perspecting.obie.model.Token;

public class RegexEntityRecognitionModel {

	private static final String createViewSymbolhistogram = "CREATE VIEW histogram_symbols AS SELECT predicate, count(DISTINCT object) FROM index_literals, symbols WHERE ( index = symbols.object ) GROUP BY predicate";
	private static final String createTableRegexDistribution = "CREATE TABLE literals_regex_distribution(regex varchar(100), property int, ratio float)";

	private static final String dropTable = "DROP TABLE IF EXISTS literals_regex_distribution CASCADE";
	private static final String dropView = "DROP VIEW IF EXISTS histogram_symbols CASCADE";

	private final Logger log = Logger
			.getLogger(RegexEntityRecognitionModel.class.getName());
	private KnowledgeBase kb;
	private String[] regexs;

	public RegexEntityRecognitionModel(String[] regexs, KnowledgeBase kb) {
		this.kb = kb;
		this.regexs = regexs;
	}

	private void resetDatabase() throws SQLException {
		Connection conn = kb.getConnection();
		conn.setAutoCommit(false);

		conn.createStatement().executeUpdate(dropTable);
		// conn.createStatement().executeUpdate(dropView);
		conn.createStatement().executeUpdate(createTableRegexDistribution);
		// conn.createStatement().executeUpdate(createViewSymbolhistogram);

		conn.commit();
		conn.setAutoCommit(true);
	}

	public void train() throws Exception {
		resetDatabase();
		for (String regex : regexs) {
			// regex = "([1-2][0-9][0-9][0-9])\\-([0-2][0-9])\\-([0-9][0-9])";

			String sql = "INSERT INTO literals_regex_distribution "
					+ " ("
					+ "SELECT '"+regex+"', H.P, H.C*1.0 / count "
					+ "FROM ( "
					+ "SELECT DISTINCT predicate AS P, count(DISTINCT object) AS C "
					+ "FROM index_literals, symbols "
					+ "WHERE literal ~ '"
					+ regex
					+ "' AND index = symbols.object GROUP BY predicate) as H, histogram_symbols "
					+ "WHERE H.P = predicate AND H.C*1.0 / count > 0.9 )";
			log.info(sql);
			kb.getConnection().createStatement().executeUpdate(sql);
		}
	}

	public TIntDoubleHashMap getDatatypeProperties(String regex) throws SQLException {
		String sql = "SELECT * FROM literals_regex_distribution WHERE regex = '"
				+ regex + "'";
		TIntDoubleHashMap result = new TIntDoubleHashMap();
		ResultSet rs = kb.getConnection().createStatement().executeQuery(sql);
		while (rs.next()) {
			result.put(rs.getInt(2), rs.getDouble(3));
		}
		rs.close();

		return result;
	}

	public String[] getRegexs() {
		return regexs;
	}

	public void transduce(Document document, String regex) {

		Pattern pattern = Pattern.compile(regex);
		String text = document.getPlainTextContent();
		Matcher matcher = pattern.matcher(text);
		while (matcher.find()) {
			int start = matcher.start();
			int end = matcher.end();
			List<Token> token = document.getTokens(start, end);

			for (int i = 0; i < token.size(); i++) {

				String position = "I";

				if (i == 0) {
					position = "B";
				}
				token.get(i).addRegexMatch(position, regex);
			}
		}
	}

}

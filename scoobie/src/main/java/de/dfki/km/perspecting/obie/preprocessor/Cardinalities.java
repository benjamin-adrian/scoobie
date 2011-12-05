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

package de.dfki.km.perspecting.obie.preprocessor;

import gnu.trove.TIntDoubleHashMap;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import de.dfki.km.perspecting.obie.connection.KnowledgeBase;

public class Cardinalities {

	private final String sql1 = "DROP TABLE IF EXISTS SUBJECT_CARD_RELATIONS CASCADE";
	private final String sql2 = "DROP TABLE IF EXISTS OBJECT_CARD_RELATIONS CASCADE";
	
	private final String query01 = "CREATE TABLE SUBJECT_CARD_RELATIONS (predicate integer, count integer, sum numeric, ratio numeric)";
	
	private final String query02 = "CREATE TABLE OBJECT_CARD_RELATIONS (predicate integer, count integer, sum numeric, ratio numeric)";
	
	private final String query1 = "INSERT INTO SUBJECT_CARD_RELATIONS "
			+ "  SELECT H.predicate, count(distinct H.subject),"
			+ "         sum(H.C), sum(H.C)/count(distinct H.subject)"
			+ "  FROM ( SELECT subject, predicate, count(*) AS C FROM RELATIONS"
			+ "  GROUP BY subject, predicate) AS H GROUP BY H.predicate";
	
	private final String query2 = "INSERT INTO OBJECT_CARD_RELATIONS " + 
			"  SELECT H.predicate, count(distinct H.object)," + 
			"         sum(H.C), sum(H.C)/count(distinct H.object)" + 
			"  FROM ( SELECT object, predicate, count(*) AS C FROM RELATIONS" + 
			"  GROUP BY object, predicate) AS H GROUP BY H.predicate";
	private KnowledgeBase kb;
	
	public Cardinalities(KnowledgeBase kb) {
		this.kb = kb;
	}
	
	public void train() throws Exception {

		try {
			Connection conn = kb.getConnection();

			Statement statement = conn.createStatement();
			statement.addBatch(sql1);
			statement.addBatch(sql2);
			statement.addBatch(query01);
			statement.addBatch(query02);
			statement.addBatch(query1);
			statement.addBatch(query2);
			statement.executeBatch();
			
			statement.close();
			conn.commit();
		} catch (SQLException e) {
			e.printStackTrace();
			e.getNextException().printStackTrace();
		}
		
	}
	
	TIntDoubleHashMap cache = new TIntDoubleHashMap();
	
	public void cache() throws Exception {
		String sql = "SELECT predicate, ratio FROM subject_card_relations ";
		Connection conn = kb.getConnection();
		Statement statement = conn.createStatement();
		ResultSet rs = statement.executeQuery(sql);
		while(rs.next()) {
			cache.put(rs.getInt(1), rs.getDouble(2));
		}
		rs.close();
		statement.close();
	}
	
	public double getSubjectCardiniality(int p) {
		return cache.get(p);
	}
	

}

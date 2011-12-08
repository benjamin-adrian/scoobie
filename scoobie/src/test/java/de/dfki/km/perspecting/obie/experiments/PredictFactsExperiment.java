/*
    Copyright (c) 2011, 
    Benjamin Adrian <benjamin.horak@gmail.com>
    
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

package de.dfki.km.perspecting.obie.experiments;

/**
 * 
 */
import gnu.trove.TIntHashSet;
import gnu.trove.TIntIntHashMap;
import gnu.trove.TObjectIntHashMap;
import gnu.trove.TObjectIntProcedure;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.net.URI;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.postgresql.jdbc2.optional.PoolingDataSource;

import cern.colt.matrix.DoubleMatrix1D;
import cern.colt.matrix.DoubleMatrix2D;
import de.dfki.km.perspecting.obie.connection.KnowledgeBase;
import de.dfki.km.perspecting.obie.connection.PostgresKB;
import de.dfki.km.perspecting.obie.connection.RemoteCursor;
import de.dfki.km.perspecting.obie.model.DoubleMatrix;
import de.dfki.km.perspecting.obie.workflow.Pipeline;

public class PredictFactsExperiment {

	/******************* technical setup ******************************************/

	private static PoolingDataSource pool = new PoolingDataSource();

	private static Pipeline pipeline;

	/******************* technical setup ******************************************/

	private static String $PHD_HOME = "/home/adrian/Dokumente/diss/";
	private static String $SCOOBIE_HOME = $PHD_HOME + "scoobie/";
	private static String $CORPUS_HOME = $PHD_HOME + "textcorpus/";

	private static String $DATABASE = "dbpedia_en2";

	private static String $DATABASE_SERVER = "pc-4327.kl.dfki.de";
	private static String $DATABASE_SERVER_USER = "postgres";
	private static String $DATABASE_SERVER_PW = "scoobie";
	private static int $DATABASE_SERVER_PORT = 5432;

	private static KnowledgeBase kb;

	/**
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {


		pool.setUser($DATABASE_SERVER_USER);
		pool.setPassword($DATABASE_SERVER_PW);
		pool.setPortNumber($DATABASE_SERVER_PORT);
		pool.setDatabaseName($DATABASE);
		pool.setServerName($DATABASE_SERVER);
		pool.setMaxConnections(100);

		kb = new PostgresKB(pool.getConnection(), $DATABASE, new URI("http://test.de"));
	}

	@AfterClass
	public static void shutdownAfterClass() {
		pool.close();
	}

	TObjectIntHashMap<String> graph1 = new TObjectIntHashMap<String>();
	TObjectIntHashMap<String> graph2 = new TObjectIntHashMap<String>();

	private void add(int subject, int predicate, int object) throws Exception {
		// kb.getURI(subject);
		// kb.getURI(predicate);
		// kb.getURI(object);
		graph1.adjustOrPutValue(String.format("%s;%s;%s", subject, predicate,
				object), 1, 1);
		graph2.adjustOrPutValue(String.format("%s;%s", subject, object), 1, 1);
	}

	@Test
	public void testCreateMarkovChain() throws Exception {

		Connection conn = pool.getConnection();
		PreparedStatement pstmt = conn
				.prepareStatement("SELECT * FROM classifications WHERE instance = ?");

		int limit = 10;
		for (int cluster : kb.getClusters()) {
			RemoteCursor rs1 = kb.getInstancesOfTypes(cluster, limit);
			System.out.println("Received instances for clusters.");
			TIntHashSet instances = new TIntHashSet();
			while (rs1.next()) {
				instances.add(rs1.getInt(1));
			}
			rs1.close();

			RemoteCursor rs2 = kb
					.getOutgoingRelations(instances.toArray());
			System.out.println("Received outgoing links instances.");
			while (rs2.next()) {
				int s = rs2.getInt(1);
				int p = rs2.getInt(2);
				int o = rs2.getInt(3);
				if (p != 10531131 && p != 9300878) {
					pstmt.setInt(1, o);
					ResultSet rs = pstmt.executeQuery();
					System.out.println(s + " received types of link's object: "
							+ o);
					while (rs.next()) {
						int type = rs.getInt(2);
						add(cluster, p, type);
					}
					rs.close();
				}
			}
		}

		serializeMarkovChain(graph1, new File( $SCOOBIE_HOME + "results/"+ $DATABASE + "/markov_chain_" + limit + ".dot"));
		// serializeMarkovChain(graph2);

		pstmt.close();
		conn.close();
	}

	private void serializeMarkovChain(TObjectIntHashMap<String> graph, File path) throws Exception {

		final BufferedWriter bw = new BufferedWriter(new FileWriter(path));
		
		final TIntIntHashMap amount = new TIntIntHashMap();

		graph.forEachEntry(new TObjectIntProcedure<String>() {

			@Override
			public boolean execute(String a, int b) {
				amount
						.adjustOrPutValue(Integer.parseInt(a.split(";")[0]), b,
								b);
				;
				return true;
			}

		});

		bw.append("digraph {");
		bw.newLine();
		
		graph.forEachEntry(new TObjectIntProcedure<String>() {

			@Override
			public boolean execute(String a, int b) {
				String[] spo = a.split(";");

				try {
					String s = kb.getURI(Integer.parseInt(spo[0]));
					String p = kb.getURI(Integer.parseInt(spo[1]));
					String o = kb.getURI(Integer.parseInt(spo[2]));
					
					
					
					bw.append(strip(s)
							+ "->"
							+ strip(o)
							+ "[label=\""
							+ strip(p)
							+ "\", weight=\""
							+ (((double) b) / amount.get(Integer
									.parseInt(spo[0]))) + "\"];");
					bw.newLine();
					bw.flush();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				return true;
			}
		});
		
		bw.append("}");
		bw.newLine();
		bw.close();
	}

	private String strip(String uri) {
		int i = uri.lastIndexOf("#");
		if (i < 0)
			i = uri.lastIndexOf("/");

		return uri.substring(i + 1).replaceAll("-", "");
	}

	@SuppressWarnings("unchecked")
	/**
	 * Test method for
	 * {@link de.dfki.km.perspecting.obie.dixi.service.SimpleScobieService#extractInformationFromURL(java.lang.String, java.lang.String)}
	 * .
	 */
	@Test
	public void testPredictFacts() {

		DoubleMatrix m = new DoubleMatrix();

		int DANA = 0;
		int DAMIAN = 1;
		int TRESTO = 2;
		int LEIF = 3;

		int KNOWS_DANA = 0;
		int KNOWS_DAMIAN = 1;
		int KNOWS_TRESTO = 2;
		int KNOWS_LEIF = 3;

		int IS_PERSON = 10;
		int IS_MAGICIAN = 11;
		int IS_WARRIOR = 12;
		int IS_LIBRIAN = 13;
		int IS_MUSICIAN = 14;
		int IS_SPIRITE = 15;

		m.add(IS_PERSON, DAMIAN, 2.0);
		m.add(IS_PERSON, TRESTO, 2.0);
		m.add(IS_PERSON, LEIF, 2.0);
		// m.add(IS_PERSON, DANA, 1);

		m.add(KNOWS_DANA, DAMIAN, 1.0);
		m.add(KNOWS_DANA, TRESTO, 1.0);
		m.add(KNOWS_DANA, LEIF, 1.0);

		m.add(KNOWS_DAMIAN, TRESTO, 1.0);
		m.add(KNOWS_DAMIAN, LEIF, 1.0);
		m.add(KNOWS_DAMIAN, DANA, 1.0);

		m.add(KNOWS_TRESTO, DAMIAN, 1.0);
		m.add(KNOWS_TRESTO, LEIF, 1.0);
		m.add(KNOWS_TRESTO, DANA, 1.0);

		m.add(KNOWS_LEIF, DAMIAN, 1.0);
		m.add(KNOWS_LEIF, TRESTO, 1.0);
		m.add(KNOWS_LEIF, DANA, 1.0);

		m.add(IS_MAGICIAN, DANA, 2.0);
		m.add(IS_MUSICIAN, DANA, 2.0);

		m.add(IS_MAGICIAN, DAMIAN, 2.0);
		m.add(IS_SPIRITE, DAMIAN, 2.0);
		m.add(IS_LIBRIAN, DAMIAN, 2.0);

		m.add(IS_WARRIOR, LEIF, 2.0);

		// reduce dimensions
		DoubleMatrix2D mc = (m.toColt());
		for (int c = 0; c < mc.columns(); c++) {
			DoubleMatrix1D col = mc.viewColumn(c);
			if (col.cardinality() == 1) {
				col.assign(0);
			}
		}

		// DoubleMatrix2D cor =
		// Statistic.correlation(Statistic.covariance(Algebra.DEFAULT.transpose(mc)));
		// DoubleMatrix2D p = m.predictValues(cor, mc);
		DoubleMatrix2D p = m.predictValuesByCosine(m.cosineSimilarity(), mc);

		System.out.println(m.toColt());
		System.out.println(mc);
		System.out.println(m.cosineSimilarity());
		System.out.println(p);

		for (int i = 0; i < m.getRowKeys().length; i++) {
			DoubleMatrix1D row = p.viewRow(i);

			for (int j = 0; j < row.size(); j++) {
				if (row.get(j) != 0) {
					System.out.println(m.getRowKeys()[i] + " "
							+ m.getColKeys()[j] + " - " + row.get(j));
				}
			}
		}

	}
	
	@Test
	public void testFactPredictor() {
		
	}
	
}

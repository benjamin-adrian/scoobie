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

import gnu.trove.TIntHashSet;
import gnu.trove.TIntIntHashMap;
import gnu.trove.TObjectIntHashMap;
import gnu.trove.TObjectIntProcedure;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.logging.Logger;

import de.dfki.km.perspecting.obie.connection.KnowledgeBase;
import de.dfki.km.perspecting.obie.connection.ResultSetCallback;

public class MarkovChain {

	private final Logger log = Logger.getLogger(MarkovChain.class.getName());
	
	private final KnowledgeBase kb;
	private final TObjectIntHashMap<String> graph1 = new TObjectIntHashMap<String>();

	private final String sql1 = "DROP TABLE IF EXISTS markov_chain CASCADE";
	private final String sql2 = "CREATE TABLE markov_chain ( subject integer,"
			+ "  predicate integer, object integer,"
			+ "  probability double precision)";

	private final int sampleCount;
	private int[] blackListedProperties;

	public MarkovChain(KnowledgeBase kb, int sampleCount,
			int[] blackListedProperties) {
		this.sampleCount = sampleCount;
		this.kb = kb;
		this.blackListedProperties = blackListedProperties;
	}

	private void add(int subject, int predicate, int object) throws Exception {
		graph1.adjustOrPutValue(String.format("%s;%s;%s", subject, predicate,
				object), 1, 1);
	}
	
	public double getProbability(int subject, int predicate, int object) throws Exception {
		
		Connection conn = kb.getConnection();
		ResultSet resultSet = conn.createStatement().executeQuery("SELECT probability FROM markov_chain WHERE (subject = "+subject+" AND predicate = "+predicate+" AND object = "+object+")");
		try {
			double p = 0.0;
			while (resultSet.next()) {
				p = resultSet.getDouble(1);
			}
			return p;
		} finally {
			resultSet.close();
		}
	}
	
//	PreparedStatement pstmt = null;
	
	public List<double[]> getMaxProbability(int subject, int object, int k) throws Exception {
		
//		List<double[]> l = new ArrayList<double[]>(k);
//		if(pstmt == null)  {
//			Connection conn = kb.getConnection();
//			pstmt = conn.prepareStatement("SELECT predicate, probability FROM markov_chain WHERE (subject = ? AND object = ?) ORDER BY probability DESC LIMIT ?");
//		}
//		
//		pstmt.setInt(1, subject);
//		pstmt.setInt(2, object);
//		pstmt.setInt(3, k);
//		
//		ResultSet resultSet = pstmt.executeQuery();
//		try {
//			while (resultSet.next()) {
//				double[] d = new double[2];
//				d[0] = resultSet.getInt(1);
//				d[1] = resultSet.getDouble(1);
//				l.add(d);
//			}
//			return l;
//		} finally {
//			resultSet.close();
//		}
		List<double[]> l = new ArrayList<double[]>(k);
		ArrayList<String> list = cache.get(String.format("%s;%s", subject, object));
		if(list != null) {
			for(int i = 0; i < Math.min(list.size(), k); i++) {
				String[]  v = list.get(i).split(";");
				double[] d = new double[2];
				d[0] = Integer.parseInt(v[0]);
				d[1] = Double.parseDouble(v[1]);
				l.add(d);
			}
		}
		return l;
 		
	}
	
	HashMap<String, ArrayList<String>> cache = new HashMap<String, ArrayList<String>>();
	
	public void cache() throws Exception {
		Statement stmt = kb.getConnection().createStatement();
		ResultSet rs = stmt.executeQuery("SELECT * FROM markov_chain ORDER BY subject, object, probability DESC");
		while(rs.next()) {
			int s = rs.getInt(1);
			int o = rs.getInt(3);
			String key = String.format("%s;%s", s,o);
			int p = rs.getInt(2);
			double w = rs.getDouble(4);
			
			ArrayList<String> list = cache.get(key);
			if(list == null) {
				list = new ArrayList<String>();
				cache.put(key, list);
			}
			list.add(String.format(Locale.ENGLISH,"%s;%1.5f", p,w));
		}
		rs.close();
		stmt.close();
	}
	
	public void train() throws Exception {

		Connection conn = kb.getConnection();

		Statement statement = conn.createStatement();
		statement.addBatch(sql1);
		statement.addBatch(sql2);
		statement.executeBatch();

		PreparedStatement pstmt = conn
				.prepareStatement("SELECT * FROM classifications WHERE instance = ?");
		// p != 10531131 && p != 9300878
		TIntHashSet blacklist = new TIntHashSet(blackListedProperties);

		for (int cluster : kb.getClusters()) {
			ResultSetCallback rs1 = kb
					.getInstancesOfTypes(cluster, sampleCount);
			log.info("Received instances for clusters.");
			TIntHashSet instances = new TIntHashSet();
			while (rs1.getRs().next()) {
				instances.add(rs1.getRs().getInt(1));
			}
			rs1.close();

			ResultSetCallback rs2 = kb
					.getOutgoingRelations(instances.toArray());
			log.info("Received outgoing links instances.");
			while (rs2.getRs().next()) {
				int s = rs2.getRs().getInt(1);
				int p = rs2.getRs().getInt(2);
				int o = rs2.getRs().getInt(3);
				if (!blacklist.contains(p)) {
					pstmt.setInt(1, o);
					ResultSet rs = pstmt.executeQuery();
					log.info(s + " received types of link's object: "
							+ o);
					while (rs.next()) {
						int type = rs.getInt(2);
						add(cluster, p, type);
					}
					rs.close();
				}
			}
		}

		// serializeMarkovChain(graph1, new File($SCOOBIE_HOME + "results/"+
		// $DATABASE + "/markov_chain_" + limit + ".dot"));
		pstmt.close();
		serializeMarkovChain();
	}

	private void serializeMarkovChain() throws Exception {

		Connection conn = kb.getConnection();
		conn.setAutoCommit(false);
		final PreparedStatement pstmt = conn
				.prepareStatement("INSERT INTO markov_chain VALUES (?, ?, ?, ?)");

		final TIntIntHashMap amount = new TIntIntHashMap();

		graph1.forEachEntry(new TObjectIntProcedure<String>() {

			@Override
			public boolean execute(String a, int b) {
				amount
						.adjustOrPutValue(Integer.parseInt(a.split(";")[0]), b,
								b);
				return true;
			}

		});

		graph1.forEachEntry(new TObjectIntProcedure<String>() {

			@Override
			public boolean execute(String a, int b) {
				String[] spo = a.split(";");

				try {
//					String s = kb.getURI(Integer.parseInt(spo[0]));
//					String p = kb.getURI(Integer.parseInt(spo[1]));
//					String o = kb.getURI(Integer.parseInt(spo[2]));

					pstmt.setInt(1, Integer.parseInt(spo[0]));
					pstmt.setInt(2, Integer.parseInt(spo[1]));
					pstmt.setInt(3, Integer.parseInt(spo[2]));
					pstmt.setDouble(4, ((double) b)
							/ amount.get(Integer.parseInt(spo[0])));
					pstmt.executeUpdate();
					
				} catch (Exception e) {
					e.printStackTrace();
				}
				return true;
			}
		});
		
		conn.commit();
		pstmt.close();
	}
//
//	private void serializeMarkovChain(TObjectIntHashMap<String> graph, File path)
//			throws Exception {
//
//		final BufferedWriter bw = new BufferedWriter(new FileWriter(path));
//
//		final TIntIntHashMap amount = new TIntIntHashMap();
//
//		graph.forEachEntry(new TObjectIntProcedure<String>() {
//
//			@Override
//			public boolean execute(String a, int b) {
//				amount
//						.adjustOrPutValue(Integer.parseInt(a.split(";")[0]), b,
//								b);
//				return true;
//			}
//
//		});
//
//		bw.append("digraph {");
//		bw.newLine();
//
//		graph.forEachEntry(new TObjectIntProcedure<String>() {
//
//			@Override
//			public boolean execute(String a, int b) {
//				String[] spo = a.split(";");
//
//				try {
//					String s = kb.getURI(Integer.parseInt(spo[0]));
//					String p = kb.getURI(Integer.parseInt(spo[1]));
//					String o = kb.getURI(Integer.parseInt(spo[2]));
//
//					bw.append(strip(s)
//							+ "->"
//							+ strip(o)
//							+ "[label=\""
//							+ strip(p)
//							+ "\", weight=\""
//							+ (((double) b) / amount.get(Integer
//									.parseInt(spo[0]))) + "\"];");
//					bw.newLine();
//					bw.flush();
//				} catch (Exception e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
//				return true;
//			}
//		});
//
//		bw.append("}");
//		bw.newLine();
//		bw.close();
//	}

	private String strip(String uri) {
		int i = uri.lastIndexOf("#");
		if (i < 0)
			i = uri.lastIndexOf("/");

		return uri.substring(i + 1).replaceAll("-", "");
	}

}

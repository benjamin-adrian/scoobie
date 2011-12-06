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

import gnu.trove.TIntHashSet;
import gnu.trove.TIntIntHashMap;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.openrdf.model.vocabulary.RDF;

import de.dfki.km.perspecting.obie.connection.RDFTripleParser.TripleStats;
import de.dfki.km.perspecting.obie.vocabulary.MediaType;

public class PostgresKB implements KnowledgeBase {

	private static final Logger log = Logger.getLogger(PostgresKB.class
			.getName());

	private final String session;

	private final Connection connection;

	private URI uri;

	public PostgresKB(Connection connection, String session, URI uri)
			throws Exception {
		this.uri = uri;
		this.session = session;
		this.connection = connection;
	}

	@Override
	public URI getUri() {
		return uri;
	}

	@Override
	public Connection getConnection() {
		return connection;
	}

	public String getSession() {
		return session;
	}

	private ResultSet executeQuery(String sql) throws Exception {
		long start = System.currentTimeMillis();
		try {
			return connection.createStatement().executeQuery(sql);
		} catch (SQLException e) {
			log.log(Level.SEVERE, "an error occurred in executing SQL query: "
					+ sql, e);
			throw e;
		} finally {
			log.fine("query " + sql + " took: "
					+ (System.currentTimeMillis() - start));
		}
	}

	private ResultSet executeQuery(PreparedStatement stmt, String sql)
			throws Exception {
		long start = System.currentTimeMillis();
		try {
			return stmt.executeQuery();
		} catch (Exception e) {
			log.log(Level.SEVERE, "an error occurred in executing SQL query: "
					+ sql, e);
			throw e;
		} finally {
			log.fine("query " + sql + " took: "
					+ (System.currentTimeMillis() - start));
		}
	}

	@Override
	public ResultSetCallback getDatatypePropertyValues(
			int datatypePropertyIndex, int rdfType) throws Exception {
		String sql = "SELECT DISTINCT index_literals.literal, index_literals.index, symbols.belief "
				+ "FROM index_literals, symbols, relations "
				+ "WHERE (symbols.belief = 1.0 AND symbols.predicate = ? "
				+ "AND symbols.object = index_literals.index AND symbols.subject = relations.subject "
				+ "AND relations.object = ?) "
				+ "ORDER BY index_literals.literal";
		try {
			PreparedStatement stmtGetTypedDatatypePropertyValues = connection
					.prepareStatement(sql);

			stmtGetTypedDatatypePropertyValues.setInt(1, datatypePropertyIndex);
			stmtGetTypedDatatypePropertyValues.setInt(2, rdfType);
			return new ResultSetCallback(executeQuery(
					stmtGetTypedDatatypePropertyValues, sql));

		} catch (SQLException e) {
			log.log(Level.SEVERE, "an error occurred in executing SQL query: "
					+ sql, e);
			throw e;
		}
	}

	@Override
	public ResultSetCallback getDatatypePropertyValues(
			int[] datatypePropertyFilter, int[] prefixes) throws Exception {
		StringBuilder sql = new StringBuilder();

		sql
				.append("SELECT DISTINCT LOWER(index_literals.literal), index_literals.index, symbols.predicate, symbols.belief, index_literals.literal "
						+ "FROM index_literals, symbols "
						+ "WHERE ( "
						+ "symbols.object = index_literals.index AND "
						+ "index_literals.prefix IN (");
		
		for (int p : prefixes) {
			sql.append("(?) , ");
		}

		sql.setLength(sql.length() - 3);

		if (datatypePropertyFilter.length == 0) {
			sql.append(")) ORDER BY LOWER(index_literals.literal)");
		} else {
			sql.append(") AND " + "symbols.predicate IN (");
			for (int p : datatypePropertyFilter) {
				sql.append("(" + p + ") , ");
			}
			sql.setLength(sql.length() - 3);
			sql.append(")) ORDER BY LOWER(index_literals.literal)");
		}

		try {

			PreparedStatement stmtGetDatatypePropertyValues = connection
					.prepareStatement(sql.toString());

			int paramIndex = 0;

			// Insert the prefixes into the query
			for (Integer p : prefixes) {
				stmtGetDatatypePropertyValues.setInt(++paramIndex, p);
			}

			return new ResultSetCallback(executeQuery(
					stmtGetDatatypePropertyValues, sql.toString()));

		} catch (SQLException e) {
			log.log(Level.SEVERE, "an error occurred in executing SQL query: "
					+ sql, e);
			throw e;
		}
	}

	public ResultSetCallback getInstanceCandidates(
			Map<Integer, Set<Integer>> literalKeys) throws Exception {

		final StringBuilder literalFilter = new StringBuilder();
		int size = 0;
		for (Set<Integer> set : literalKeys.values()) {
			size += set.size();
		}

		for (int l : literalKeys.keySet()) {
			for (int p : literalKeys.get(l)) {
				literalFilter.append("(");
				literalFilter.append(p);
				literalFilter.append(",");
				literalFilter.append(l);
				literalFilter.append(")");
				literalFilter.append(" ,  ");
			}
		}

		if (literalFilter.length() == 0) {
			return null;
		}
		final String sql = "SELECT DISTINCT symbols.subject, symbols.predicate, symbols.object, index_resources.uri "
				+ "FROM symbols, index_resources "
				+ "WHERE (symbols.subject = index_resources.index AND (symbols.predicate, symbols.object) IN ("
				+ literalFilter.substring(0, literalFilter.length() - 4) + "))";
		try {
			final PreparedStatement stmtGetInstanceCandidates = connection
					.prepareStatement(sql);

			final ResultSet rs = executeQuery(stmtGetInstanceCandidates, sql);

			return new ResultSetCallback(rs);

		} catch (SQLException e) {
			log.log(Level.SEVERE, "an error occurred in executing SQL query: "
					+ sql, e);
			throw e;
		}
	}

	public ResultSetCallback dbSort(List<CharSequence> index, int maxLength)
			throws Exception {

		String prefix = "SELECT * FROM ( VALUES ";
		String suffix = " ) AS t(string) order by string";

		StringBuilder b = new StringBuilder();
		for (int i = 0; i < index.size(); i++) {
			b.append("(?)");
			b.append(",");
		}
		String sql;
		if (b.length() > 0) {
			sql = prefix + b.substring(0, b.length() - 1) + suffix;
		} else {
			return null;
		}

		try {
			PreparedStatement pstmt = connection.prepareStatement(sql);

			for (int i = 0; i < index.size(); i++) {
				int min = Math.min(maxLength, index.get(i).length());
				pstmt.setString(i + 1, ((String) index.get(i).subSequence(0,
						min)).toLowerCase());
			}

			ResultSet rs = executeQuery(pstmt, sql);
			return new ResultSetCallback(rs);
		} catch (Exception e) {
			log.log(Level.SEVERE, "an error occurred in executing SQL query: "
					+ sql, e);
			throw e;
		}
	}

	private PreparedStatement stmtGetLiteralIndex = null;

	public int getLiteralIndex(String literal) throws Exception {
		int result = -1;
		final String sql = "SELECT index_literals.index FROM index_literals WHERE (index_literals.literal = ?)";

		try {
			if (stmtGetLiteralIndex == null)
				stmtGetLiteralIndex = connection.prepareStatement(sql);

			stmtGetLiteralIndex.setString(1, literal);

			ResultSet rs = executeQuery(stmtGetLiteralIndex, sql);
			if (rs.next()) {
				result = rs.getInt(1);
			}
			rs.close();
		} catch (Exception e) {
			log.log(Level.SEVERE, "an error occurred in executing SQL query: "
					+ sql, e);
			throw e;
		}
		if (result == -1) {
			Exception e = new Exception("literal: " + literal
					+ " is not indexed");
			log.warning("an error occurred in executing SQL query: " + sql
					+ ". " + e.getMessage());
			throw e;
		} else {
			return result;
		}
	}

	PreparedStatement stmtGetURI = null;

	public String getURI(int index) throws Exception {
		ResultSet rs = null;
		String result = null;
		String sql = "SELECT index_resources.uri FROM index_resources WHERE (index_resources.index = ?)";
		try {
			if (stmtGetURI == null)
				stmtGetURI = connection.prepareStatement(sql);

			stmtGetURI.setInt(1, index);

			rs = executeQuery(stmtGetURI, sql);
			if (rs.next()) {
				result = rs.getString(1);
			}
			rs.close();
			if (result == null)
				throw new Exception("URI with index: " + index
						+ " is not indexed");
			else
				return result;
		} catch (Exception e) {
			log.log(Level.SEVERE, "an error occurred in executing SQL query: "
					+ sql, e);
			throw e;
		}
	}

	private PreparedStatement stmtGetLiteral = null;

	public String getLiteral(int index) throws Exception {

		String result = null;
		final String sql = "SELECT index_literals.literal FROM index_literals WHERE (index_literals.index = ?)";
		try {
			if (stmtGetLiteralIndex == null)
				stmtGetLiteral = connection.prepareStatement(sql);

			stmtGetLiteral.setInt(1, index);

			ResultSet rs = executeQuery(stmtGetLiteral, sql);
			if (rs.next()) {
				result = rs.getString(1);
			}
			rs.close();
		} catch (Exception e) {
			log.log(Level.SEVERE, "an error occurred in executing SQL query: "
					+ sql, e);
			throw e;
		}
		if (result == null) {
			throw new Exception("Literal with index: " + index
					+ " is not indexed");
		} else {
			return result;
		}
	}

	PreparedStatement stmtGetUriIndex = null;

	public int getUriIndex(String uri) throws Exception {
		ResultSet rs = null;
		int result = -1;

		final String sql = "SELECT index_resources.index FROM index_resources WHERE (index_resources.uri = ?)";

		try {
			if (stmtGetUriIndex == null)
				stmtGetUriIndex = connection.prepareStatement(sql);

			stmtGetUriIndex.setString(1, uri);

			rs = executeQuery(stmtGetUriIndex, sql);
			if (rs.next())
				result = rs.getInt("index");
			rs.close();
		} catch (Exception e) {
			log.log(Level.SEVERE, "an error occurred in executing SQL query: "
					+ sql, e);
			throw e;
		}
		if (result == -1) {
			throw new Exception("uri: " + uri + " is not indexed");
		} else {
			return result;
		}

	}

	PreparedStatement stmtGetOutgoingRelations = null;

	@Override
	public int[] getOutgoingRelations(int instance, int relation)
			throws Exception {
		final String sql = "SELECT relations.object FROM relations WHERE (relations.subject = ? AND relations.predicate = ?)";
		final TIntHashSet s = new TIntHashSet();

		try {
			if (relation == -1) {
				for (Set<Integer> s1 : getOutgoingRelations(instance).values()) {
					for (int r : s1) {
						s.add(r);
					}
				}
			} else {

				if (stmtGetOutgoingRelations == null) {
					stmtGetOutgoingRelations = connection.prepareStatement(sql);
				}
				stmtGetOutgoingRelations.setInt(1, instance);
				stmtGetOutgoingRelations.setInt(2, relation);

				ResultSet rs = executeQuery(stmtGetOutgoingRelations, sql);

				while (rs.next()) {
					s.add(rs.getInt(1));
				}
				rs.close();
			}
		} catch (Exception e) {
			log.log(Level.SEVERE, "an error occurred in executing SQL query: "
					+ sql, e);
			throw e;
		}
		return s.toArray();
	}

	PreparedStatement stmtGetAllOutgoingRelations = null;

	public Map<Integer, Set<Integer>> getOutgoingRelations(int instance)
			throws Exception {
		HashMap<Integer, Set<Integer>> relations = new HashMap<Integer, Set<Integer>>();
		String sql = "SELECT relations.predicate, relations.object FROM relations WHERE (relations.subject = ?)";

		try {

			if (stmtGetAllOutgoingRelations == null) {
				stmtGetAllOutgoingRelations = connection.prepareStatement(sql);
			}

			stmtGetAllOutgoingRelations.setInt(1, instance);

			ResultSet rs = executeQuery(stmtGetAllOutgoingRelations, sql);

			while (rs.next()) {

				int property = rs.getInt(1);

				Set<Integer> mask = relations.get(property);
				if (mask == null) {
					mask = new HashSet<Integer>();
					relations.put(property, mask);
				}
				mask.add(rs.getInt(2));
			}
			rs.close();
			return relations;
		} catch (Exception e) {
			log.log(Level.SEVERE, "an error occurred in executing SQL query: "
					+ sql, e);
			throw e;
		}
	}

	public ResultSetCallback getOutgoingRelations(int[] instances)
			throws Exception {
		final StringBuilder b = new StringBuilder();
		for (int i : instances) {
			b.append('(');
			b.append(i);
			b.append(')');
			b.append(',');
		}

		String sql = "SELECT relations.subject, relations.predicate, relations.object FROM relations WHERE (relations.subject IN ("
				+ b.substring(0, b.length() - 1) + "))";
		try {
			PreparedStatement pstmt = connection.prepareStatement(sql);
			ResultSet rs = executeQuery(pstmt, sql);
			return new ResultSetCallback(rs);
		} catch (Exception e) {
			log.log(Level.SEVERE, "an error occurred in executing SQL query: "
					+ sql, e);
			throw e;
		}
	}

	public ResultSetCallback getIncomingRelations(int[] instances)
			throws Exception {
		StringBuilder b = new StringBuilder();
		for (int i : instances) {
			b.append('(');
			b.append(i);
			b.append(')');
			b.append(',');
		}

		String qs;

		if (instances.length == 0) {
			return null;
		} else {
			qs = b.substring(0, b.length() - 1);
		}

		String sql = "SELECT relations.subject, relations.predicate, relations.object "
				+ "FROM relations "
				+ "WHERE (relations.object IN ("
				+ qs
				+ ")) ORDER BY relations.subject";
		try {
			PreparedStatement pstmt = connection.prepareStatement(sql);
			ResultSet rs = executeQuery(pstmt, sql);
			return new ResultSetCallback(rs);
		} catch (Exception e) {
			log.log(Level.SEVERE, "an error occurred in executing SQL query: "
					+ sql, e);
			throw e;
		}
	}

	private PreparedStatement stmtGetIncomingRelations = null;

	@Override
	public int[] getIncomingRelations(int instance, int relation)
			throws Exception {
		String sql = "SELECT relations.subject FROM relations WHERE (relations.object = ? AND relations.predicate = ?)";
		TIntHashSet s = new TIntHashSet();

		try {
			if (relation == -1) {
				for (Set<Integer> s1 : getIncomingRelations(instance).values()) {
					for (int r : s1) {
						s.add(r);
					}
				}
				return s.toArray();
			} else {

				if (stmtGetIncomingRelations == null) {
					stmtGetIncomingRelations = connection.prepareStatement(sql);
				}
				stmtGetIncomingRelations.setInt(1, instance);
				stmtGetIncomingRelations.setInt(2, relation);

				ResultSet rs = executeQuery(stmtGetIncomingRelations, sql);

				while (rs.next()) {
					s.add(rs.getInt(1));
				}
				rs.close();
			}
		} catch (Exception e) {
			log.log(Level.SEVERE, "an error occurred in executing SQL query: "
					+ sql, e);
			throw e;
		}
		return s.toArray();

	}

	private PreparedStatement stmtGetAllIncomingRelations = null;

	public Map<Integer, Set<Integer>> getIncomingRelations(int instance)
			throws Exception {

		HashMap<Integer, Set<Integer>> relations = new HashMap<Integer, Set<Integer>>();

		String sql = "SELECT relations.predicate, relations.subject FROM relations WHERE (relations.object = ?)";

		try {
			if (stmtGetAllIncomingRelations == null) {
				stmtGetAllIncomingRelations = connection.prepareStatement(sql);
			}
			stmtGetAllIncomingRelations.setInt(1, instance);

			ResultSet rs = executeQuery(stmtGetAllIncomingRelations, sql);

			while (rs.next()) {

				int property = rs.getInt(1);

				Set<Integer> mask = relations.get(property);
				if (mask == null) {
					mask = new HashSet<Integer>();
					relations.put(property, mask);
				}
				mask.add(rs.getInt(2));
			}
			rs.close();
		} catch (Exception e) {
			log.log(Level.SEVERE, "an error occurred in executing SQL query: "
					+ sql, e);
			throw e;
		}
		return relations;

	}

	int typeIndex = -1;

	@Override
	public ResultSetCallback getRDFTypesForInstances(int[] subjects)
			throws Exception {

		if (typeIndex == -1) {
			typeIndex = getUriIndex(RDF.TYPE.toString());
		}

		StringBuilder b = new StringBuilder();
		for (int i : subjects) {
			b.append('(');
			b.append(i);
			b.append(')');
			b.append(',');
		}

		String sql = "SELECT subject, object FROM relations WHERE (subject IN ("
				+ b.substring(0, b.length() - 1)
				+ ") AND predicate = "
				+ typeIndex + ")";
		try {
			PreparedStatement pstmt = connection.prepareStatement(sql);

			ResultSet rs = executeQuery(pstmt, sql);

			return new ResultSetCallback(rs);
		} catch (Exception e) {
			log.log(Level.SEVERE, "an error occurred in executing SQL query: "
					+ sql, e);
			throw e;
		}
	}

	PreparedStatement typePstmt = null;

	@Override
	public ResultSetCallback getInstancesOfTypes(int type, int count)
			throws Exception {

		if (typeIndex == -1) {
			typeIndex = getUriIndex(RDF.TYPE.toString());
		}

		String sql = "SELECT subject FROM relations WHERE (object = ? AND predicate = "
				+ typeIndex + ") LIMIT " + count;
		try {
			if (typePstmt == null) {
				typePstmt = connection.prepareStatement(sql);
			}
			typePstmt.setInt(1, type);
			ResultSet rs = executeQuery(typePstmt, sql);

			return new ResultSetCallback(rs);
		} catch (Exception e) {
			log.log(Level.SEVERE, "an error occurred in executing SQL query: "
					+ sql, e);
			throw e;
		}
	}

	@Override
	public ResultSetCallback getRDFTypes() throws Exception {
		
		if (typeIndex == -1) {
			typeIndex = getUriIndex(RDF.TYPE.toString());
		}

		String sql = "SELECT DISTINCT object FROM relations WHERE (predicate = "
				+ typeIndex + ")";
		try {
			PreparedStatement pstmt = connection.prepareStatement(sql);
			ResultSet rs = executeQuery(pstmt, sql);

			return new ResultSetCallback(rs);
		} catch (Exception e) {
			log.log(Level.SEVERE, "an error occurred in executing SQL query: "
					+ sql, e);
			throw e;
		}
	}

	public ResultSetCallback getLiteralLengthHistogram() throws Exception {
		String sql = "SELECT LENGTH(literal), COUNT(LENGTH(literal)) FROM INDEX_LITERALS GROUP BY LENGTH(literal) ORDER BY LENGTH(literal)";
		try {
			PreparedStatement pstmt1 = connection.prepareStatement(sql);
			ResultSet rs = executeQuery(pstmt1, sql);
			return new ResultSetCallback(rs);
		} catch (Exception e) {
			log.log(Level.SEVERE, "an error occurred in executing SQL query: "
					+ sql, e);
			throw e;
		}
	}

	@Override
	protected void finalize() throws Throwable {
		connection.close();
		super.finalize();
	}

	@Override
	public int[] getClusters() throws Exception {
		String sql = "SELECT DISTINCT cluster FROM type_clusters";
		TIntHashSet clusters = new TIntHashSet();
		try {
			ResultSet rs = executeQuery(sql);
			while (rs.next()) {
				clusters.add(rs.getInt(1));
			}
			rs.close();
		} catch (Exception e) {
			log.log(Level.SEVERE, "an error occurred in executing SQL query: "
					+ sql, e);
			throw e;
		}
		return clusters.toArray();
	}

	private PreparedStatement getDatatypePropertyByClass = null;

	@Override
	public int[] getDatatypePropertyByClass(int cluster, double t)
			throws Exception {

		TIntHashSet properties = new TIntHashSet();

		String sql = "SELECT property, rating FROM proper_noun_rating WHERE (cluster = ? AND rating > ?)";

		try {
			if (getDatatypePropertyByClass == null) {
				getDatatypePropertyByClass = connection.prepareStatement(sql);
			}

			getDatatypePropertyByClass.setInt(1, cluster);
			getDatatypePropertyByClass.setDouble(2, t);

			ResultSet rs = executeQuery(getDatatypePropertyByClass, sql);
			while (rs.next()) {
				properties.add(rs.getInt(1));
			}
			rs.close();
		} catch (Exception e) {
			log.log(Level.SEVERE, "an error occurred in executing SQL query: "
					+ sql, e);
			throw e;
		}

		return properties.toArray();
	}

	private PreparedStatement getConnectingClusters = null;

	@Override
	public Collection<int[]> getConnectingClusters(int objectProperty, double t)
			throws Exception {

		ArrayList<int[]> clusters = new ArrayList<int[]>();

		String sql = "SELECT subject, object, probability FROM markov_chain WHERE (predicate = ? AND probability > ?)";

		try {
			if (getConnectingClusters == null) {
				getConnectingClusters = connection.prepareStatement(sql);
			}

			getConnectingClusters.setInt(1, objectProperty);
			getConnectingClusters.setDouble(2, t);

			ResultSet rs = executeQuery(getConnectingClusters, sql);
			while (rs.next()) {
				clusters.add(new int[] { rs.getInt(1), rs.getInt(2) });
			}
			rs.close();
		} catch (Exception e) {
			log.log(Level.SEVERE, "an error occurred in executing SQL query: "
					+ sql, e);
			throw e;
		}

		return clusters;
	}

	@Override
	public int getCluster(int[] types) throws Exception {

		StringBuilder buf = new StringBuilder();
		buf.append("(");
		int maxIndex = types.length - 1;
		for (int i = 0; i <= maxIndex; i++) {
			buf.append(types[i]);
			if (i < maxIndex)
				buf.append(", ");
		}
		buf.append(")");

		String sql = "SELECT DISTINCT cluster FROM type_clusters WHERE type IN "
				+ buf.toString();

		int bestCluster;
		try {
			ResultSet rs = executeQuery(sql);

			TIntIntHashMap setCovering = new TIntIntHashMap();
			while (rs.next()) {
				setCovering.adjustOrPutValue(rs.getInt(1), 1, 1);
			}
			rs.close();

			int max = 0;
			bestCluster = -1;

			for (int c : setCovering.keys()) {
				int count = setCovering.get(c);

				if (max < c) {
					max = count;
					bestCluster = c;
				}
			}
		} catch (Exception e) {
			log.log(Level.SEVERE, "an error occurred in executing SQL query: "
					+ sql, e);
			throw e;
		}

		return bestCluster;
	}

	@Override
	public int getPropertyType(int property) throws Exception {

		// TODO : This is not good style
		
		String sql1 = "SELECT (count(*) > 0) FROM symbols WHERE predicate = "
				+ property;
		String sql2 = "SELECT (count(*) > 0) FROM relations WHERE predicate = "
				+ property;

		ResultSet rs = executeQuery(sql1);
		rs.next();
		boolean checkType = rs.getBoolean(1);
		rs.close();
		if (checkType)
			return 1;
		rs = executeQuery(sql2);
		rs.next();
		checkType = rs.getBoolean(1);
		if (checkType)
			return 2;
		else
			return 0;
	}

	private void uploadBulk(File file, String table, String session,
			Connection conn) throws Exception {

		final Statement bulkImport = conn.createStatement();
		// If we are on windows machines, we need to replace the backslashes.
		// To be consistent, we always use slashes.
		String filename = file.getAbsolutePath().replace("\\", "/");
		log.info("Starting bulk import of " + filename);
		int size = bulkImport.executeUpdate("COPY " + table + " FROM '"
				+ filename + "' WITH CSV");

		bulkImport.close();
		log.info("Committed bulk import of " + filename + " #entries: " + size);

	}
	
	@Override
	public void preprocessRdfData(InputStream[] datasets, MediaType rdfMimeType, MediaType fileMimeType, String absoluteBaseURI)
			throws Exception {
		this.connection.setAutoCommit(false);
		createDatabase();
		loadRDFData(datasets, rdfMimeType, absoluteBaseURI, fileMimeType);
		connection.commit();
		createIndexes();
		connection.commit();
		connection.close();
	}
	


	private void createIndexes() throws Exception {

		try {
			Statement s = connection.createStatement();
						
			InputStream in = PostgresKB.class.getResourceAsStream("indexscheme.sql");
			BufferedReader reader = new BufferedReader(new InputStreamReader(in));
			
			StringBuilder builder = new StringBuilder();
			for(String line = reader.readLine(); line != null; line = reader.readLine()) {
				builder.append(line);
				builder.append("\n");
			}
			reader.close();
			in.close();
			String sqlBatch = builder.toString();
			for (String sql : sqlBatch.split(";\n")) {
				s.addBatch(sql);
			}
			s.executeBatch();
			s.close();
			log.info("Created indexes: " + connection.getCatalog());
		} catch (SQLException e) {
			log.log(Level.SEVERE, PostgresKB.class.getName(), e);
			throw new Exception(e);
		}
	}
	
	private void createDatabase() throws Exception {

		try {
			Statement s = connection.createStatement();
						
			InputStream in = PostgresKB.class.getResourceAsStream("dbscheme.sql");
			BufferedReader reader = new BufferedReader(new InputStreamReader(in));
			
			StringBuilder builder = new StringBuilder();
			for(String line = reader.readLine(); line != null; line = reader.readLine()) {
				builder.append(line);
				builder.append("\n");
			}
			reader.close();
			in.close();
			String sqlBatch = builder.toString();
			for (String sql : sqlBatch.split(";\n")) {
				s.addBatch(sql);
			}
			s.executeBatch();
			s.close();
			connection.commit();
			log.info("Created scheme: " + connection.getCatalog());
		} catch (SQLException e) {
			log.log(Level.SEVERE, PostgresKB.class.getName(), e);
			throw new Exception(e);
		}
	}
	
	private void loadRDFData(InputStream[] instanceBases,
			MediaType rdfMimeType, String absoluteBaseURI, MediaType fileMimeType)
			throws Exception {
		final ExecutorService pool = Executors.newCachedThreadPool();
		
		
		log.info("Parsing RDF dump files: ... ");
		long start = System.currentTimeMillis();
		RDFTripleParser parser = new RDFTripleParser();

		File.createTempFile(session, "").mkdir();
		
		
		
		final TripleStats tripleStats = parser.parseTriples(instanceBases,
				rdfMimeType, new File(System.getProperty("java.io.tmpdir")), absoluteBaseURI, fileMimeType);
		log.info("[done] took " + (System.currentTimeMillis() - start) + "ms");

		Future<?> f1 = pool.submit(new Runnable() {
			@Override
			public void run() {
				try {
					uploadBulk(tripleStats.objectProps, "TMP_RELATIONS", session, connection);
				} catch (Exception e) {
					log.log(Level.SEVERE, PostgresKB.class.getName(), e);
				}
			}
		});
		
		Future<?> f2 = pool.submit(new Runnable() {
			@Override
			public void run() {
				try {
					uploadBulk(tripleStats.datatypeProps, "TMP_SYMBOLS", session, connection);
				} catch (Exception e) {
					log.log(Level.SEVERE, PostgresKB.class.getName(), e);
				}
			}
		});
		
		f1.get();
		f2.get();
		
		Future<?> f3 = pool.submit(new Runnable() {
			@Override
			public void run() {
				try {
					storeResourceIndex();
				} catch (Exception e) {
					log.log(Level.SEVERE, PostgresKB.class.getName(), e);
				}
			}
		});
		
		Future<?> f4 = pool.submit(new Runnable() {
			@Override
			public void run() {
				try {
					storeLiteralValues();
				} catch (Exception e) {
					log.log(Level.SEVERE, PostgresKB.class.getName(), e);
				}
			}
		});
		
		f3.get();
		f4.get();
		
		Future<?> f5 = pool.submit(new Runnable() {
			@Override
			public void run() {
				try {
					initDatatypePropertyValues();
				} catch (Exception e) {
					log.log(Level.SEVERE, PostgresKB.class.getName(), e);
				}
			}
		});
		
		Future<?> f6 = pool.submit(new Runnable() {
			@Override
			public void run() {
				try {
					initObjectPropertyValues();
				} catch (Exception e) {
					log.log(Level.SEVERE, PostgresKB.class.getName(), e);
				}
			}
		});
		
		f5.get();
		f6.get();
		
		dropTMP();

	}



	private void storeResourceIndex() throws Exception {

		log.info("Populating table: index_resources");
		int subjects = connection.prepareStatement(
				"INSERT INTO tmp_index_resources (uri) "
						+ "SELECT DISTINCT s FROM tmp_relations").executeUpdate();

		log.info("Added " + subjects + " subjects to table: index_resources");

		subjects += connection.prepareStatement(
				"INSERT INTO tmp_index_resources (uri) "
						+ "SELECT DISTINCT p FROM tmp_relations").executeUpdate();
		log.info("Added " + subjects
				+ " predicates to index_resources");

		subjects += connection.prepareStatement(
				"INSERT INTO tmp_index_resources (uri) "
						+ "SELECT DISTINCT o FROM tmp_relations").executeUpdate();
		log.info("Added " + subjects + " objects to index_resources");

		subjects += connection.prepareStatement(
				"INSERT INTO tmp_index_resources (uri) "
						+ "SELECT DISTINCT p FROM tmp_symbols").executeUpdate();
		log.info("Added " + subjects
				+ " datatype predicates to index_resources");
		subjects += connection.prepareStatement(
				"INSERT INTO tmp_index_resources (uri) "
						+ "SELECT DISTINCT s FROM tmp_symbols").executeUpdate();

		log.info("Added " + subjects
				+ " datatype subjects to tmp_index_resources");

		subjects += connection.prepareStatement(
				"INSERT INTO index_resources (uri) "
						+ "SELECT DISTINCT uri FROM tmp_index_resources")
				.executeUpdate();

		log.info("Added " + subjects
				+ " datatype subjects to index_resources");


		log.info("Dropping tmp_index_resources");
		connection.prepareStatement("DROP TABLE tmp_index_resources").execute();
		
		log.info("Finished population query for index_resources");
		log.info("Committed population of index_resources");
		log.info(" ... stored " + subjects + " resources.");

	}

	private void dropTMP() throws SQLException {
		log.info("Dropping tmp_relations");
		connection.prepareStatement("DROP TABLE tmp_relations").execute();
		log.info("Dropping tmp_symbols");
		connection.prepareStatement("DROP TABLE tmp_symbols").execute();
		log.info("Committed dropping");
	}

	private void storeLiteralValues() throws SQLException {
		final PreparedStatement cleanInsert = connection
		.prepareStatement("INSERT INTO index_literals (literal, prefix) " +
				"SELECT DISTINCT o as literal, h as prefix FROM tmp_symbols");
		log.info("Populating index_literals");
		int i = cleanInsert.executeUpdate();
		cleanInsert.close();

		log.info("Finished population query for index_literals");
		log.info("Committed population of index_literals");
		log.info(" ... stored " + i + " literals.");

	}

	private void initObjectPropertyValues() throws Exception {

		final PreparedStatement stmt = connection
				.prepareStatement("INSERT INTO relations "
						+ " (subject, predicate, object) "
						+ "SELECT A.index AS subject, B.index AS predicate, C.index AS object " +
								"FROM index_resources A, index_resources B, index_resources C, tmp_relations D " +
								"WHERE(A.uri = D.s AND B.uri = D.p AND C.uri = D.o) ");

		int updateCount = stmt.executeUpdate();
		log.info("Added " + updateCount
				+ " triples with object properties");
	}

	private void initDatatypePropertyValues() throws Exception {
		final PreparedStatement stmt = connection
				.prepareStatement("INSERT INTO symbols "
						+ "(subject, predicate, object, belief) "
						+ "SELECT DISTINCT A.index AS subject, B.index AS predicate, C.index AS object, 1.0 AS belief "
						+ "FROM index_resources A, index_resources B, index_literals C, tmp_symbols D "
						+ "WHERE (A.uri = D.s AND B.uri = D.p AND C.literal = D.o) ");

		int updateCount = stmt.executeUpdate();

		log.info("Added " + updateCount
				+ " triples with datatype properties");
	}

}

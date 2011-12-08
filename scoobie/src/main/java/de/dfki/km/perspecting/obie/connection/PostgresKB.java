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

import gnu.trove.TIntDoubleHashMap;
import gnu.trove.TIntHashSet;
import gnu.trove.TIntIntHashMap;
import gnu.trove.TIntObjectHashMap;
import gnu.trove.TIntObjectProcedure;
import gnu.trove.TObjectIntHashMap;
import gnu.trove.TObjectIntProcedure;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.sql.BatchUpdateException;
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
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.openrdf.model.vocabulary.RDF;

import cern.colt.matrix.DoubleMatrix1D;
import cern.colt.matrix.DoubleMatrix2D;
import cern.colt.matrix.doublealgo.Statistic;

import de.dfki.km.perspecting.obie.connection.RDFTripleParser.TripleStats;
import de.dfki.km.perspecting.obie.corpus.TextCorpus;
import de.dfki.km.perspecting.obie.model.DoubleMatrix;
import de.dfki.km.perspecting.obie.vocabulary.MediaType;
import de.dfki.km.perspecting.obie.workflow.Pipeline;

public class PostgresKB implements KnowledgeBase {

	private static final Logger log = Logger.getLogger(PostgresKB.class
			.getName());

	
	protected String INDEXSCHEME_SQL = "postgres/indexscheme.sql";
	protected String DBSCHEME_SQL = "postgres/dbscheme.sql";
	protected String session;
	protected Connection connection;
	protected URI uri;

	public PostgresKB(Connection connection, String session, URI uri)
			throws Exception {
		this(connection, session, uri, "postgres/dbscheme.sql", "postgres/indexscheme.sql");
	}
	
	protected PostgresKB(Connection connection, String session, URI uri, String dbSchema, String indexSchema)
			throws Exception {
		this.uri = uri;
		this.session = session;
		this.connection = connection;
		this.INDEXSCHEME_SQL = indexSchema;
		this.DBSCHEME_SQL = dbSchema;
	}

	@Override
	public URI getUri() {
		return uri;
	}

	public String getSession() {
		return session;
	}

	protected ResultSet executeQuery(String sql) throws Exception {
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

	protected ResultSet executeQuery(PreparedStatement stmt, String sql)
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
	public RemoteCursor getDatatypePropertyValues(int datatypePropertyIndex,
			int rdfType) throws Exception {
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
			return new ResultSetCursor(executeQuery(
					stmtGetTypedDatatypePropertyValues, sql));

		} catch (SQLException e) {
			log.log(Level.SEVERE, "an error occurred in executing SQL query: "
					+ sql, e);
			throw e;
		}
	}

	@Override
	public ResultSetCursor getDatatypePropertyValues(
			int[] datatypePropertyFilter, int[] prefixes) throws Exception {
		StringBuilder sql = new StringBuilder();

		sql.append("SELECT DISTINCT LOWER(index_literals.literal), index_literals.index, symbols.predicate, symbols.belief, index_literals.literal "
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

			return new ResultSetCursor(executeQuery(
					stmtGetDatatypePropertyValues, sql.toString()));

		} catch (SQLException e) {
			log.log(Level.SEVERE, "an error occurred in executing SQL query: "
					+ sql, e);
			throw e;
		}
	}

	public ResultSetCursor getInstanceCandidates(
			Map<Integer, Set<Integer>> literalKeys) throws Exception {

		final StringBuilder literalFilter = new StringBuilder();

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

			return new ResultSetCursor(rs);

		} catch (SQLException e) {
			log.log(Level.SEVERE, "an error occurred in executing SQL query: "
					+ sql, e);
			throw e;
		}
	}

	public ResultSetCursor dbSort(List<CharSequence> index, int maxLength)
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
				pstmt.setString(i + 1,
						((String) index.get(i).subSequence(0, min))
								.toLowerCase());
			}

			ResultSet rs = executeQuery(pstmt, sql);
			return new ResultSetCursor(rs);
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

	public ResultSetCursor getOutgoingRelations(int[] instances)
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
			return new ResultSetCursor(rs);
		} catch (Exception e) {
			log.log(Level.SEVERE, "an error occurred in executing SQL query: "
					+ sql, e);
			throw e;
		}
	}

	public ResultSetCursor getIncomingRelations(int[] instances)
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
			return new ResultSetCursor(rs);
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
	public ResultSetCursor getRDFTypesForInstances(int[] subjects)
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

			return new ResultSetCursor(rs);
		} catch (Exception e) {
			log.log(Level.SEVERE, "an error occurred in executing SQL query: "
					+ sql, e);
			throw e;
		}
	}

	PreparedStatement typePstmt = null;

	@Override
	public ResultSetCursor getInstancesOfTypes(int type, int count)
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

			return new ResultSetCursor(rs);
		} catch (Exception e) {
			log.log(Level.SEVERE, "an error occurred in executing SQL query: "
					+ sql, e);
			throw e;
		}
	}

	@Override
	public ResultSetCursor getRDFTypes() throws Exception {

		if (typeIndex == -1) {
			typeIndex = getUriIndex(RDF.TYPE.toString());
		}

		String sql = "SELECT DISTINCT object FROM relations WHERE (predicate = "
				+ typeIndex + ")";
		try {
			PreparedStatement pstmt = connection.prepareStatement(sql);
			ResultSet rs = executeQuery(pstmt, sql);

			return new ResultSetCursor(rs);
		} catch (Exception e) {
			log.log(Level.SEVERE, "an error occurred in executing SQL query: "
					+ sql, e);
			throw e;
		}
	}

	public RemoteCursor getLiteralLengthHistogram() throws Exception {
		String sql = "SELECT LENGTH(literal), COUNT(LENGTH(literal)) FROM INDEX_LITERALS GROUP BY LENGTH(literal) ORDER BY LENGTH(literal)";
		try {
			PreparedStatement pstmt1 = connection.prepareStatement(sql);
			ResultSet rs = executeQuery(pstmt1, sql);
			return new ResultSetCursor(rs);
		} catch (Exception e) {
			log.log(Level.SEVERE, "an error occurred in executing SQL query: "
					+ sql, e);
			throw e;
		}
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

	protected void uploadBulk(File file, String table, String session,
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
	public void preprocessRdfData(InputStream[] datasets,
			MediaType rdfMimeType, MediaType fileMimeType,
			String absoluteBaseURI) throws Exception {
		this.connection.setAutoCommit(false);
		createDatabase();
		loadRDFData(datasets, rdfMimeType, absoluteBaseURI, fileMimeType);
		connection.commit();
		createIndexes();
		connection.commit();
	}

	protected void createIndexes() throws Exception {

		try {
			Statement s = connection.createStatement();

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

	protected void createDatabase() throws Exception {

		InputStream in = PostgresKB.class.getResourceAsStream(DBSCHEME_SQL);
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
		try {
			Statement s = connection.createStatement();

			for (String sql : sqlBatch.split(";\n")) {
				s.addBatch(sql);
			}
			int[] batches = s.executeBatch();
			s.close();
			connection.commit();
			log.info("Created scheme: " + connection.getCatalog());
		} catch(BatchUpdateException e1) {
			int count = 0;
			for(int i : e1.getUpdateCounts()) {
				
				switch(i) {
					case Statement.SUCCESS_NO_INFO: System.out.println("Succeeded "+ sqlBatch.split(";\n")[count].trim());
					case Statement.EXECUTE_FAILED: System.out.println("Failed "+ sqlBatch.split(";\n")[count].trim());
					default :System.out.println("Succeeded with "+i+" update counts: "+ sqlBatch.split(";\n")[count].trim());
				}
				count++;
			}
			System.out.println("Failed "+ sqlBatch.split(";\n")[count].trim());
			
			throw new Exception(printSQLException(e1));

		} catch (SQLException e) {
			printSQLException(e);
			log.log(Level.SEVERE, PostgresKB.class.getName(), e);
			throw new Exception(printSQLException(e));
		}
	}

	private void loadRDFData(InputStream[] instanceBases,
			MediaType rdfMimeType, String absoluteBaseURI,
			MediaType fileMimeType) throws Exception {
		final ExecutorService pool = Executors.newCachedThreadPool();

		log.info("Parsing RDF dump files: ... ");
		long start = System.currentTimeMillis();
		RDFTripleParser parser = new RDFTripleParser();

		File.createTempFile(session, "").mkdir();

		final TripleStats tripleStats = parser.parseTriples(instanceBases,
				rdfMimeType, new File(System.getProperty("java.io.tmpdir")),
				absoluteBaseURI, fileMimeType);
		log.info("[done] took " + (System.currentTimeMillis() - start) + "ms");

		Future<?> f1 = pool.submit(new Runnable() {
			@Override
			public void run() {
				try {
					uploadBulk(tripleStats.objectProps, "TMP_RELATIONS",
							session, connection);
				} catch (Exception e) {
					log.log(Level.SEVERE, PostgresKB.class.getName(), e);
				}
			}
		});

		Future<?> f2 = pool.submit(new Runnable() {
			@Override
			public void run() {
				try {
					uploadBulk(tripleStats.datatypeProps, "TMP_SYMBOLS",
							session, connection);
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
						+ "SELECT DISTINCT s FROM tmp_relations")
				.executeUpdate();

		log.info("Added " + subjects + " subjects to table: index_resources");

		subjects += connection.prepareStatement(
				"INSERT INTO tmp_index_resources (uri) "
						+ "SELECT DISTINCT p FROM tmp_relations")
				.executeUpdate();
		log.info("Added " + subjects + " predicates to index_resources");

		subjects += connection.prepareStatement(
				"INSERT INTO tmp_index_resources (uri) "
						+ "SELECT DISTINCT o FROM tmp_relations")
				.executeUpdate();
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

		log.info("Added " + subjects + " datatype subjects to index_resources");

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
				.prepareStatement("INSERT INTO index_literals (literal, prefix) "
						+ "SELECT DISTINCT o as literal, h as prefix FROM tmp_symbols");
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
						+ "SELECT A.index AS subject, B.index AS predicate, C.index AS object "
						+ "FROM index_resources A, index_resources B, index_resources C, tmp_relations D "
						+ "WHERE(A.uri = D.s AND B.uri = D.p AND C.uri = D.o) ");

		int updateCount = stmt.executeUpdate();
		log.info("Added " + updateCount + " triples with object properties");
	}

	private void initDatatypePropertyValues() throws Exception {
		final PreparedStatement stmt = connection
				.prepareStatement("INSERT INTO symbols "
						+ "(subject, predicate, object, belief) "
						+ "SELECT DISTINCT A.index AS subject, B.index AS predicate, C.index AS object, 1.0 AS belief "
						+ "FROM index_resources A, index_resources B, index_literals C, tmp_symbols D "
						+ "WHERE (A.uri = D.s AND B.uri = D.p AND C.literal = D.o) ");

		int updateCount = stmt.executeUpdate();

		log.info("Added " + updateCount + " triples with datatype properties");
	}

	public void calculateCardinalities() throws Exception {

		connection.createStatement().executeUpdate(
				"DELETE FROM TABLE SUBJECT_CARD_RELATIONS");
		connection.createStatement().executeUpdate(
				"DELETE FROM TABLE OBJECT_CARD_RELATIONS");
		connection.commit();
		log.info("cleared tables SUBJECT_CARD_RELATIONS, OBJECT_CARD_RELATIONS.");

		final String query1 = "INSERT INTO SUBJECT_CARD_RELATIONS "
				+ "  SELECT H.predicate, count(distinct H.subject),"
				+ "         sum(H.C), sum(H.C)/count(distinct H.subject)"
				+ "  FROM ( SELECT subject, predicate, count(*) AS C FROM RELATIONS"
				+ "  GROUP BY subject, predicate) AS H GROUP BY H.predicate";

		final String query2 = "INSERT INTO OBJECT_CARD_RELATIONS "
				+ "  SELECT H.predicate, count(distinct H.object),"
				+ "         sum(H.C), sum(H.C)/count(distinct H.object)"
				+ "  FROM ( SELECT object, predicate, count(*) AS C FROM RELATIONS"
				+ "  GROUP BY object, predicate) AS H GROUP BY H.predicate";
		Statement statement = connection.createStatement();
		statement.addBatch(query1);
		statement.addBatch(query2);
		statement.executeBatch();
		statement.close();
		connection.commit();
	}

	private TIntDoubleHashMap subjectCardinialityCache = new TIntDoubleHashMap();

	public double getSubjectCardinality(int p) throws Exception {

		if (subjectCardinialityCache.isEmpty()) {
			String sql = "SELECT predicate, ratio FROM subject_card_relations ";
			Statement statement = connection.createStatement();
			ResultSet rs = statement.executeQuery(sql);
			while (rs.next()) {
				subjectCardinialityCache.put(rs.getInt(1), rs.getDouble(2));
			}
			rs.close();
			statement.close();
		}

		return subjectCardinialityCache.get(p);
	}

	public void calculateMarkovChain(int[] blackListedProperties,
			int sampleCount) throws Exception {
		final TObjectIntHashMap<String> graph1 = new TObjectIntHashMap<String>();

		connection.createStatement().executeUpdate(
				"DELETE FROM TABLE markov_chain");
		connection.commit();
		log.info("cleared table markov_chain.");

		Statement statement = connection.createStatement();
		statement.executeBatch();

		final PreparedStatement pstmt = connection
				.prepareStatement("SELECT * FROM classifications WHERE instance = ?");
		// p != 10531131 && p != 9300878
		TIntHashSet blacklist = new TIntHashSet(blackListedProperties);

		for (int cluster : getClusters()) {
			RemoteCursor rs1 = getInstancesOfTypes(cluster, sampleCount);
			log.info("Received instances for clusters.");
			TIntHashSet instances = new TIntHashSet();
			while (rs1.next()) {
				instances.add(rs1.getInt(1));
			}
			rs1.close();

			RemoteCursor rs2 = getOutgoingRelations(instances.toArray());
			log.info("Received outgoing links instances.");
			while (rs2.next()) {
				int s = rs2.getInt(1);
				int p = rs2.getInt(2);
				int o = rs2.getInt(3);
				if (!blacklist.contains(p)) {
					pstmt.setInt(1, o);
					ResultSet rs = pstmt.executeQuery();
					log.info(s + " received types of link's object: " + o);
					while (rs.next()) {
						int type = rs.getInt(2);

						graph1.adjustOrPutValue(
								String.format("%s;%s;%s", cluster, p, type), 1,
								1);

					}
					rs.close();
				}
			}
		}

		// serializeMarkovChain(graph1, new File($SCOOBIE_HOME + "results/"+
		// $DATABASE + "/markov_chain_" + limit + ".dot"));
		pstmt.close();

		connection.setAutoCommit(false);
		final PreparedStatement pstmt1 = connection
				.prepareStatement("INSERT INTO markov_chain VALUES (?, ?, ?, ?)");

		final TIntIntHashMap amount = new TIntIntHashMap();

		graph1.forEachEntry(new TObjectIntProcedure<String>() {

			@Override
			public boolean execute(String a, int b) {
				amount.adjustOrPutValue(Integer.parseInt(a.split(";")[0]), b, b);
				return true;
			}

		});

		graph1.forEachEntry(new TObjectIntProcedure<String>() {

			@Override
			public boolean execute(String a, int b) {
				String[] spo = a.split(";");

				try {

					pstmt1.setInt(1, Integer.parseInt(spo[0]));
					pstmt1.setInt(2, Integer.parseInt(spo[1]));
					pstmt1.setInt(3, Integer.parseInt(spo[2]));
					pstmt1.setDouble(4,
							((double) b) / amount.get(Integer.parseInt(spo[0])));
					pstmt1.executeUpdate();

				} catch (Exception e) {
					e.printStackTrace();
				}
				return true;
			}
		});

		connection.commit();
		pstmt1.close();

	}

	private HashMap<String, ArrayList<String>> markovChainCache = new HashMap<String, ArrayList<String>>();

	public double getMarkovProbability(int subject, int predicate, int object)
			throws Exception {

		ResultSet resultSet = connection.createStatement().executeQuery(
				"SELECT probability FROM markov_chain WHERE (subject = "
						+ subject + " AND predicate = " + predicate
						+ " AND object = " + object + ")");
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

	public List<double[]> getMaxMarkovProbability(int subject, int object, int k)
			throws Exception {

		if (markovChainCache.isEmpty()) {
			Statement stmt = connection.createStatement();
			ResultSet rs = stmt
					.executeQuery("SELECT * FROM markov_chain ORDER BY subject, object, probability DESC");
			while (rs.next()) {
				int s = rs.getInt(1);
				int o = rs.getInt(3);
				String key = String.format("%s;%s", s, o);
				int p = rs.getInt(2);
				double w = rs.getDouble(4);

				ArrayList<String> list = markovChainCache.get(key);
				if (list == null) {
					list = new ArrayList<String>();
					markovChainCache.put(key, list);
				}
				list.add(String.format(Locale.ENGLISH, "%s;%1.5f", p, w));
			}
			rs.close();
			stmt.close();
		}

		List<double[]> l = new ArrayList<double[]>(k);
		ArrayList<String> list = markovChainCache.get(String.format("%s;%s",
				subject, object));
		if (list != null) {
			for (int i = 0; i < Math.min(list.size(), k); i++) {
				String[] v = list.get(i).split(";");
				double[] d = new double[2];
				d[0] = Integer.parseInt(v[0]);
				d[1] = Double.parseDouble(v[1]);
				l.add(d);
			}
		}
		return l;

	}

	public TIntObjectHashMap<TIntObjectHashMap<double[]>> getCoverageAmbiguity()
			throws Exception {

		final String SELECT_COVERAGE_AMBIGUITY = "SELECT C.attribute, C.coverage, A.avg_references FROM ( "
				+ "SELECT S.predicate AS attribute, (count(DISTINCT S.subject) / avg(HT.count)) AS coverage "
				+ "FROM relations R, symbols S, histogram_types HT "
				+ "WHERE ( HT.type = R.object AND S.subject = R.subject AND R.object = ?) "
				+ "GROUP BY S.predicate ) "
				+ "C, AMBIGUITY_SYMBOLS A "
				+ "WHERE ( A.attribute = C.attribute )";

		TIntObjectHashMap<TIntObjectHashMap<double[]>> result = new TIntObjectHashMap<TIntObjectHashMap<double[]>>();

		PreparedStatement stmt = connection
				.prepareStatement(SELECT_COVERAGE_AMBIGUITY);

		for (int typeIndex : getClusters()) {

			TIntObjectHashMap<double[]> propertyValues = new TIntObjectHashMap<double[]>();
			stmt.setInt(1, typeIndex);

			ResultSet rs = stmt.executeQuery();
			while (rs.next()) {
				int propery = rs.getInt(1);
				double coverage = rs.getDouble(2);
				double ambiguity = rs.getDouble(3);
				propertyValues.put(propery,
						new double[] { coverage, ambiguity });
			}
			rs.close();

			result.put(typeIndex, propertyValues);
		}
		stmt.close();

		return result;
	}

	public void calculateProperNameStatistics(TextCorpus corpus, Pipeline pipe)
			throws Exception {

		connection.createStatement().executeUpdate(
				"DELETE FROM TABLE proper_noun_rating");
		connection.commit();

		final TIntDoubleHashMap propertyIDF = corpus.getDocumentFrequency(pipe);

		final TIntObjectHashMap<TIntObjectHashMap<double[]>> typePropertyCoverageAmbiguity = getCoverageAmbiguity();

		final String sql = "INSERT INTO proper_noun_rating VALUES (?,?,?,?,?,?)";

		final PreparedStatement pstmt = connection.prepareStatement(sql);

		typePropertyCoverageAmbiguity
				.forEachEntry(new TIntObjectProcedure<TIntObjectHashMap<double[]>>() {
					@Override
					public boolean execute(
							final int type,
							TIntObjectHashMap<double[]> propertyCoverageAmbiguity) {

						propertyCoverageAmbiguity
								.forEachEntry(new TIntObjectProcedure<double[]>() {

									@Override
									public boolean execute(int property,
											double[] coverageAmbiguity) {

										double idf = propertyIDF.get(property);

										try {
											pstmt.setInt(1, type);
											pstmt.setInt(2, property);
											pstmt.setDouble(
													3,
													coverageAmbiguity[0]
															/ coverageAmbiguity[1]
															* idf);
											pstmt.setDouble(4,
													coverageAmbiguity[0]);
											pstmt.setDouble(5,
													coverageAmbiguity[1]);
											pstmt.setDouble(6, idf);
											pstmt.executeUpdate();
										} catch (SQLException e) {
											e.printStackTrace();
										}

										return true;
									}
								});

						return true;
					}
				});

		pstmt.close();

	}

	/**
	 * @param kb
	 * @return
	 * @throws Exception
	 * @throws SQLException
	 */
	public DoubleMatrix getTypeCorrelations(int samples) throws Exception {
		Set<Integer> types = new HashSet<Integer>();
		RemoteCursor rs = getRDFTypes();
		while (rs.next()) {
			types.add(rs.getInt(1));
		}
		rs.close();
		log.info("Retrieved " + types.size() + " classes");

		TIntHashSet instances = new TIntHashSet();
		final DoubleMatrix data = new DoubleMatrix();
		for (int type : types) {
			RemoteCursor rs1 = getInstancesOfTypes(type, samples);
			while (rs1.next()) {
				instances.add(rs1.getInt(1));
			}
			rs1.close();
		}
		log.info("Calculated " + samples + " samples for " + types.size()
				+ " classes");
		HashMap<Integer, Set<Integer>> typeMap = new HashMap<Integer, Set<Integer>>();

		RemoteCursor rs2 = getRDFTypesForInstances(instances.toArray());
		while (rs2.next()) {

			Set<Integer> typeSet = typeMap.get(rs2.getInt(1));
			if (typeSet == null) {
				typeSet = new HashSet<Integer>();
				typeMap.put(rs2.getInt(1), typeSet);
			}
			typeSet.add(rs2.getInt(2));
		}
		rs2.close();

		for (Entry<Integer, Set<Integer>> e : typeMap.entrySet()) {
			for (int i1 : e.getValue()) {
				for (int i2 : e.getValue()) {
					double d = data.get(i1, i2);
					data.add(i1, i2, d + 1.0);
				}
			}
		}
		log.info("Populated matrix with cooccuring types of sample instances");

		return data;
	}

	public void clusterCorrelatingClasses(int samples, double biasThreshold,
			double pruningThreshold) throws Exception {

		connection.createStatement().executeUpdate(
				"DELETE FROM TABLE type_clusters");
		connection.commit();

		final DoubleMatrix data = getTypeCorrelations(samples);
		log.info("Calculating covariances");
		final DoubleMatrix2D cov = data.covarianceMatrix();
		log.info("Calculating correlations");
		final DoubleMatrix2D cor = Statistic.correlation(cov);
		log.info("Start hierarchical clustering");
		DoubleMatrix2D hMatrix = data.hierarchicalLabeledClustering(cor,
				biasThreshold, pruningThreshold);

		double maxValue = 0;
		int bestLabel = 0;
		TIntHashSet clusterValues;

		TIntHashSet globalClusteredValues = new TIntHashSet();

		for (int row = 0; row < hMatrix.rows(); row++) {
			DoubleMatrix1D projectedColumn = hMatrix.viewRow(row);

			clusterValues = new TIntHashSet();
			maxValue = -1;
			bestLabel = -1;

			for (int col = 0; col < hMatrix.columns(); col++) {
				if (projectedColumn.get(col) > 0) {
					clusterValues.add(data.getColKeys()[col]);
					globalClusteredValues.add(data.getColKeys()[col]);
					if (projectedColumn.get(col) > maxValue) {
						maxValue = projectedColumn.get(col);
						bestLabel = data.getColKeys()[col];
					}
				}
			}

			for (int type : clusterValues.toArray()) {
				connection.createStatement().executeUpdate(
						"INSERT INTO type_clusters VALUES (" + type + ", "
								+ bestLabel + ")");
				log.info("Clustering " + getURI(type) + " as "
						+ getURI(bestLabel));
			}
		}

		RemoteCursor rs = getRDFTypes();
		while (rs.next()) {
			int type = rs.getInt(1);
			if (!globalClusteredValues.contains(type)) {
				connection.createStatement().executeUpdate(
						"INSERT INTO type_clusters VALUES (" + type + ", "
								+ type + ")");
				log.info("Clustering " + getURI(type) + " as " + getURI(type));
			}
		}
		rs.close();

		connection.commit();
	}
	

	public void calculateRegexDistributions(String[] regexs) throws Exception {
		

		connection.createStatement().executeUpdate(
				"DELETE FROM TABLE literals_regex_distribution");
		connection.commit();
		
		
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
			connection.createStatement().executeUpdate(sql);
		}
		connection.commit();
	}

	public TIntDoubleHashMap getDatatypePropertiesForRegex(String regex) throws Exception {
		String sql = "SELECT * FROM literals_regex_distribution WHERE regex = '"
				+ regex + "'";
		TIntDoubleHashMap result = new TIntDoubleHashMap();
		ResultSet rs = connection.createStatement().executeQuery(sql);
		while (rs.next()) {
			result.put(rs.getInt(2), rs.getDouble(3));
		}
		rs.close();

		return result;
	}

	public String[] getRegexs() throws Exception {
		ArrayList<String> regexs = new ArrayList<String>();
		
		String sql = "SELECT DISTINCT regex FROM literals_regex_distribution";
		ResultSet rs = connection.createStatement().executeQuery(sql);
		while (rs.next()) {
			regexs.add(rs.getString(1));
		}
		rs.close();

		return regexs.toArray(new String[regexs.size()]);
		
	}
	
    protected String printSQLException(SQLException e) {
        // Unwraps the entire exception chain to unveil the real cause of the
        // Exception.

    	
        StringBuilder b = new StringBuilder();
        while (e != null) {
                b.append("\n----- SQLException -----");
                b.append("  SQL State:  " + e.getSQLState());
                b.append("  Error Code: " + e.getErrorCode());
                b.append("  Message:    " + e.getMessage());
                e = e.getNextException();
        }

        return b.toString();
}


}

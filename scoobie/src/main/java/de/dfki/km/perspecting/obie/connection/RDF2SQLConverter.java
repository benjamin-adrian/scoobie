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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import de.dfki.km.perspecting.obie.connection.RDFTripleParser.TripleStats;
import de.dfki.km.perspecting.obie.vocabulary.MediaType;

public class RDF2SQLConverter {

	private final static Logger log = Logger
			.getLogger(RDF2SQLConverter.class.getName());

	private static final ExecutorService pool = Executors.newCachedThreadPool();

	
	private String sessionPath;
	private String session;
	private String configPath;
	private Connection connection;
	private KnowledgeBase kb;

	public RDF2SQLConverter(KnowledgeBase kb, String configPath) {
		this.kb = kb;
		this.configPath = configPath;

	}


	private void decompressDefaultSession(InputStream input, String sessionConfigPath) {
		final int BUFFER = 2048;
		try {
			BufferedOutputStream dest = null;
			ZipInputStream zis = new ZipInputStream(
					new BufferedInputStream(input));
			ZipEntry entry;
			while ((entry = zis.getNextEntry()) != null) {
				int count;
				byte data[] = new byte[BUFFER];
				// write the files to the disk
				if(entry.isDirectory()) {
					new File(sessionConfigPath+"/"+entry.getName()).mkdir();
					continue;
				}
				
				File out = new File(sessionConfigPath+"/"+entry.getName());
				FileOutputStream fos = new FileOutputStream(out);

				dest = new BufferedOutputStream(fos, BUFFER);
				while ((count = zis.read(data, 0, BUFFER)) != -1) {
					dest.write(data, 0, count);
				}
				dest.flush();
				dest.close();
			}
			zis.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/*
	 * jdbc.driver.class = org.apache.derby.jdbc.EmbeddedDriver jdbc.protocol =
	 * "jdbc:derby:" db.user = scoobie db.password = scoobie
	 */
	private void init() throws Exception {
		this.connection = kb.getConnection();
		this.connection.setAutoCommit(false);
		sessionPath = this.configPath;
		if (!sessionPath.endsWith("/")) {
			sessionPath += "/";
		}
		sessionPath += session;
		
		createDatabase();
//		initFolderStructure();
	}


	private void createIndexes() throws Exception {

		try {
			Connection connection = kb.getConnection();
			Statement s = connection.createStatement();
						
			InputStream in = RDF2SQLConverter.class.getResourceAsStream("indexscheme.sql");
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
			log.log(Level.SEVERE, RDF2SQLConverter.class.getName(), e);
			throw new Exception(e);
		}
	}
	
	private void createDatabase() throws Exception {

		try {
			Statement s = connection.createStatement();
						
			InputStream in = RDF2SQLConverter.class.getResourceAsStream("dbscheme.sql");
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
			log.log(Level.SEVERE, RDF2SQLConverter.class.getName(), e);
			throw new Exception(e);
		}
	}

	/**
	 * Initialize the folder structure for a session.
	 * 
	 * Generates Folders and copies models.
	 * 
	 * @throws IOException
	 */
	private void initFolderStructure() throws IOException {
		if (new File(sessionPath).mkdirs()) {
			log.info("created folder " + sessionPath);
			decompressDefaultSession(RDF2SQLConverter.class.getResourceAsStream("session.zip"), sessionPath);
			log.info("created default session in " + sessionPath);
		} else {
			log.info("Folder " + sessionPath + " already exists.");
		}
	}

	/**
	 * @param datasets
	 * @param mimeType
	 * @param sessionPath2
	 * @param absoluteBaseURI
	 */
	public void preprocess(String[] datasets, MediaType rdfMimeType, MediaType fileMimeType, String session, String absoluteBaseURI) throws Exception {
		this.session = session.replaceAll("\\W", "_");
		init();
		loadRDFData(datasets, rdfMimeType, absoluteBaseURI, fileMimeType);
		connection.commit();
		createIndexes();
		connection.commit();
		connection.close();
		pool.shutdown();
	}

	private void loadRDFData(String[] instanceBases,
			MediaType rdfMimeType, String absoluteBaseURI, MediaType fileMimeType)
			throws Exception {
		
		log.info("Parsing RDF dump files: ... ");
		long start = System.currentTimeMillis();
		RDFTripleParser parser = new RDFTripleParser();

		final TripleStats tripleStats = parser.parseTriples(instanceBases,
				rdfMimeType, sessionPath, absoluteBaseURI, fileMimeType);
		log.info("[done] took " + (System.currentTimeMillis() - start) + "ms");

		Future<?> f1 = pool.submit(new Runnable() {
			@Override
			public void run() {
				try {
					kb.uploadBulk(tripleStats.objectProps, "TMP_RELATIONS", session, connection);
				} catch (Exception e) {
					log.log(Level.SEVERE, RDF2SQLConverter.class.getName(), e);
				}
			}
		});
		
		Future<?> f2 = pool.submit(new Runnable() {
			@Override
			public void run() {
				try {
					kb.uploadBulk(tripleStats.datatypeProps, "TMP_SYMBOLS", session, connection);
				} catch (Exception e) {
					log.log(Level.SEVERE, RDF2SQLConverter.class.getName(), e);
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
					log.log(Level.SEVERE, RDF2SQLConverter.class.getName(), e);
				}
			}
		});
		
		Future<?> f4 = pool.submit(new Runnable() {
			@Override
			public void run() {
				try {
					storeLiteralValues();
				} catch (Exception e) {
					log.log(Level.SEVERE, RDF2SQLConverter.class.getName(), e);
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
					log.log(Level.SEVERE, RDF2SQLConverter.class.getName(), e);
				}
			}
		});
		
		Future<?> f6 = pool.submit(new Runnable() {
			@Override
			public void run() {
				try {
					initObjectPropertyValues();
				} catch (Exception e) {
					log.log(Level.SEVERE, RDF2SQLConverter.class.getName(), e);
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

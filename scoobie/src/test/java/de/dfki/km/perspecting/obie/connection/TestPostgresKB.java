package de.dfki.km.perspecting.obie.connection;

import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;

import java.io.InputStream;
import java.net.URI;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.Arrays;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.postgresql.jdbc2.optional.PoolingDataSource;

import de.dfki.km.perspecting.obie.transducer.model.LiteralHashing;
import de.dfki.km.perspecting.obie.vocabulary.MediaType;

public class TestPostgresKB {

	/******************* technical setup ******************************************/

	private static String $DATABASE_SCOOBIE_TEST = "scoobie_test";
	private static String $DATABASE_SERVER_LOCALHOST = "localhost";
	private static String $DATABASE_SERVER_USER = "postgres";
	private static String $DATABASE_SERVER_PW = "scoobie";
	private static String $URI = "http://scoobie.org/db/example/";
	private static int $DATABASE_SERVER_PORT = 5432;

	static PoolingDataSource pool = null;
	static KnowledgeBase kb = null;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {

		pool = new PoolingDataSource();

		pool.setUser($DATABASE_SERVER_USER);
		pool.setPassword($DATABASE_SERVER_PW);
		pool.setPortNumber($DATABASE_SERVER_PORT);
		pool.setDatabaseName($DATABASE_SCOOBIE_TEST);
		pool.setServerName($DATABASE_SERVER_LOCALHOST);
		pool.setMaxConnections(10);

		kb = new PostgresKB(pool.getConnection(), $DATABASE_SCOOBIE_TEST,
				new URI($URI));

	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		pool.close();
	}

	@Test
	public void testPreprocessRdfData() {

		InputStream[] datasets = new InputStream[] {
				PostgresKB.class.getResourceAsStream("test1.ttl"),
				PostgresKB.class.getResourceAsStream("test2.ttl") };

		try {
			kb.preprocessRdfData(datasets, MediaType.TURTLE, MediaType.TEXT,
					"http://scoobie.org", new LiteralHashing(4));

			Connection conn = pool.getConnection();
			ResultSet rs = conn.createStatement().executeQuery(
					"SELECT uri FROM index_resources");

			while (rs.next()) {
				String uriFromDB = rs.getString(1);
				int key = kb.getUriIndex(uriFromDB);
				String uriFromKB = kb.getURI(key);
				assertEquals(uriFromDB, uriFromKB);
			}
			rs.close();

			rs = conn.createStatement().executeQuery(
					"SELECT literal FROM index_literals");
			while (rs.next()) {
				String literalFromDB = rs.getString(1);
				int key = kb.getLiteralIndex(literalFromDB);
				String literalFromKB = kb.getLiteral(key);
				assertEquals(literalFromDB, literalFromKB);
			}
			rs.close();
			conn.close();


		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());

		}
	}		
	
	@Test
	public void testGetRDFTypes() throws Exception {
		RemoteCursor cursor = kb.getRDFTypes();
		while (cursor.next()) {
			int typeIndex = cursor.getInt(1);
			String typeURI = kb.getURI(typeIndex);
			assertEquals(kb.getUriIndex(typeURI), typeIndex);
		}
		cursor.close();
	}
	
	@Test
	public void testGetRDFTypesForInstances() throws Exception {
		
		Connection conn = pool.getConnection();
		ResultSet rs = conn.createStatement().executeQuery(
				"SELECT uri FROM index_resources");

		while (rs.next()) {
			String uriFromDB = rs.getString(1);
			int key = kb.getUriIndex(uriFromDB);
			RemoteCursor cursor = kb.getRDFTypesForInstances(new int[]{key});
			while (cursor.next()) {
				assertEquals(cursor.getInt(1), key);
			}
			cursor.close();
		}
		rs.close();
		conn.close();
	}


	@Test
	public void testGetOutgoingRelations() throws Exception {
		Connection conn = pool.getConnection();
		ResultSet rs = conn.createStatement().executeQuery(
				"SELECT uri FROM index_resources");

		while (rs.next()) {
			String uriFromDB = rs.getString(1);
			int key = kb.getUriIndex(uriFromDB);

			RemoteCursor cursor = kb
					.getOutgoingRelations(new int[] { key });
			while (cursor.next()) {
				assertEquals(cursor.getInt(1), key);
			}
			cursor.close();

		}
		rs.close();
		conn.close();
		
	}
	
	@Test
	public void testGetIncomingRelations() throws Exception {
		Connection conn = pool.getConnection();
		ResultSet rs = conn.createStatement().executeQuery(
				"SELECT uri FROM index_resources");
		
		while (rs.next()) {
			String uriFromDB = rs.getString(1);
			int key = kb.getUriIndex(uriFromDB);

			
			RemoteCursor cursor = kb.getIncomingRelations(new int[] { key });
			while (cursor.next()) {
				assertEquals(cursor.getInt(3), key);
			}
			cursor.close();
		}
		rs.close();
		conn.close();
	}
	
	@Test
	public void testDBSort() throws Exception {
		
		RemoteCursor cursor = kb.dbSort(Arrays.asList(new String[]{"a","b","A","B"," a","á"}), 100);
		while (cursor.next()) {
			System.out.println(cursor.getString(1));
		}
		cursor.close();
		
	}

}

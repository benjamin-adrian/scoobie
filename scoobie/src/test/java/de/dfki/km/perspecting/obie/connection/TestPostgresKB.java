package de.dfki.km.perspecting.obie.connection;

import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URI;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.postgresql.jdbc2.optional.PoolingDataSource;

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
			kb.preprocessRdfData(datasets, MediaType.TURTLE, MediaType.TEXT, "http://scoobie.org");
			
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
			
		}
	}

}

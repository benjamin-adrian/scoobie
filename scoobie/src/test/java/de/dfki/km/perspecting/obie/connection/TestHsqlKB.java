package de.dfki.km.perspecting.obie.connection;

import static org.junit.Assert.fail;

import java.io.InputStream;
import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import de.dfki.km.perspecting.obie.transducer.model.LiteralHashing;
import de.dfki.km.perspecting.obie.vocabulary.MediaType;

public class TestHsqlKB {

	/******************* technical setup ******************************************/

	private static String $DATABASE_SCOOBIE_TEST = "scoobie_test";
	private static String $DATABASE_SERVER_USER = "postgres";
	private static String $DATABASE_SERVER_PW = "scoobie";
	private static String $URI = "http://scoobie.org/db/example/";


	static Connection c = null;
	static KnowledgeBase kb = null;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {

	    try {
	        Class.forName("org.hsqldb.jdbcDriver" );
	    } catch (Exception e) {
	        System.out.println("ERROR: failed to load HSQLDB JDBC driver.");
	        e.printStackTrace();
	        return;
	    }
	    c = DriverManager.getConnection("jdbc:hsqldb:file:testdb", "sa", "");
		
		kb = new HsqlKB(c, $DATABASE_SCOOBIE_TEST,
				new URI($URI));

	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
        try
        {
            Statement shutdown = c.createStatement();
            shutdown.execute( "SHUTDOWN" );
            shutdown.close();
        }
        finally
        {
            if( c != null )
                c.close();
        }

	}

	@Test
	public void testPreprocessRdfData() {
		
		InputStream[] datasets = new InputStream[] {
				PostgresKB.class.getResourceAsStream("test1.ttl"),
				PostgresKB.class.getResourceAsStream("test2.ttl") };

		try {
			kb.preprocessRdfData(datasets, MediaType.TURTLE, MediaType.TEXT, "http://scoobie.org", new LiteralHashing(4));
			
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
			
		}
	}

}

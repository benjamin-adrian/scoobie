package sandbox.perspecting;

/**
 * 
 */
import java.io.File;
import java.io.FileWriter;
import java.io.Reader;
import java.net.URI;
import java.util.Collection;

import org.apache.commons.io.IOUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.postgresql.jdbc2.optional.PoolingDataSource;

import de.dfki.km.perspecting.obie.corpus.TextCorpus;
import de.dfki.km.perspecting.obie.model.Document;
import de.dfki.km.perspecting.obie.model.DocumentProcedure;
import de.dfki.km.perspecting.obie.model.Scoobie;
import de.dfki.km.perspecting.obie.vocabulary.Language;
import de.dfki.km.perspecting.obie.vocabulary.MediaType;
import de.dfki.km.perspecting.obie.workflow.Pipeline;

public class SABDemo {

	/******************* technical setup ******************************************/

	private static String $DATABASE_DBPEDIA_en2 = "dbpedia_en2";
	private static String $DATABASE_SERVER_PC_4327 = "pc-4327.kl.dfki.de";

	private static PoolingDataSource pool = new PoolingDataSource();

	/******************* technical setup ******************************************/


	private static String $DATABASE_SERVER_USER = "postgres";
	private static String $DATABASE_SERVER_PW = "scoobie";
	private static int $DATABASE_SERVER_PORT = 5432;
	private final static String RDFS_LABEL = "http://www.w3.org/2000/01/rdf-schema#label";

	private static Scoobie $;
	private static TextCorpus corpus;

	/**
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		corpus = new TextCorpus(new File("src/test/resources/sandbox/perspecting/text.zip"),
				MediaType.ZIP, MediaType.HTML, Language.EN);

		$ = new Scoobie($DATABASE_DBPEDIA_en2, $DATABASE_SERVER_USER,
				$DATABASE_SERVER_PW, $DATABASE_SERVER_PORT,
				$DATABASE_SERVER_PC_4327, null, corpus, new URI(
						"http://dbpedia.org"));

	}

	@AfterClass
	public static void shutdownAfterClass() {
		pool.close();
	}

	@Test
	public void test() {
		try {
			final JSONBasedDBPediaSerializer json = new JSONBasedDBPediaSerializer();

			@SuppressWarnings("unchecked")
			Collection<String> results = (Collection<String>) corpus
					.forEach(new DocumentProcedure<String>() {
						@Override
						public String process(Reader doc, URI uri)
								throws Exception {

							System.out.println(uri);
//							
//							final String template = "SELECT * FROM NAMED <http://port-41xy?graph="
//									+ $.kb().getUri()
//									+ "&doc="
//									+ uri.toString()
//									+ "#recognized> WHERE {GRAPH <http://port-41xy?graph="
//									+ $.kb().getUri()
//									+ "&doc="
//									+ uri.toString()
//									+ "#recognized> {?s <" + RDFS_LABEL + "> ?o}}";
//							Document document = $.pipeline().createDocument(doc,
//									uri, MediaType.TEXT, template, Language.EN);
//							for (int step = 0;  $.pipeline().hasNext(step); step =  $.pipeline()
//									.execute(step, document)) {
//							}
//
//							Reader reader = json.serialize(document, $.kb());
//							FileWriter writer = new FileWriter(
//									"src/test/resources/sandbox/"+uri.toString().split("/")[uri.toString().split("/").length-1].replace(".zip", ""));
//							IOUtils.copy(reader, writer);
//
//							reader.close();
//							writer.close();

							return uri.toString();

						}
					});

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
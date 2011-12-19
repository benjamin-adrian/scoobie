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

import java.io.BufferedReader;
import java.io.File;
import java.net.URI;

import opennlp.tools.postag.POSModel;
import opennlp.tools.postag.POSTaggerME;

import org.apache.lucene.search.IndexSearcher;
import org.junit.AfterClass;
import org.junit.Test;
import org.postgresql.jdbc2.optional.PoolingDataSource;

import de.dfki.km.perspecting.obie.connection.KnowledgeBase;
import de.dfki.km.perspecting.obie.connection.PostgresKB;
import de.dfki.km.perspecting.obie.corpus.TextCorpus;
import de.dfki.km.perspecting.obie.corpus.WikinewsCorpus;
import de.dfki.km.perspecting.obie.model.Document;
import de.dfki.km.perspecting.obie.model.FilterContext;
import de.dfki.km.perspecting.obie.model.Scoobie;
import de.dfki.km.perspecting.obie.postprocessor.ListSerializer;
import de.dfki.km.perspecting.obie.transducer.EntityClassification;
import de.dfki.km.perspecting.obie.transducer.EntityDisambiguation;
import de.dfki.km.perspecting.obie.transducer.FactRecommender;
import de.dfki.km.perspecting.obie.transducer.InstanceRecognition;
import de.dfki.km.perspecting.obie.transducer.KnownFactsRetrieval;
import de.dfki.km.perspecting.obie.transducer.LanguageIdentification;
import de.dfki.km.perspecting.obie.transducer.POSTagging;
import de.dfki.km.perspecting.obie.transducer.ProperNameRecognition;
import de.dfki.km.perspecting.obie.transducer.RDFLiteralSpotting;
import de.dfki.km.perspecting.obie.transducer.RegularStructuredEntityRecognition;
import de.dfki.km.perspecting.obie.transducer.RelevanceRating;
import de.dfki.km.perspecting.obie.transducer.SentenceSegmenter;
import de.dfki.km.perspecting.obie.transducer.SuffixArrayBuilder;
import de.dfki.km.perspecting.obie.transducer.WordSegmenter;
import de.dfki.km.perspecting.obie.transducer.model.CRFNounPhraseChunkerModel;
import de.dfki.km.perspecting.obie.transducer.model.LiteralHashing;
import de.dfki.km.perspecting.obie.transducer.model.MaxentEntityClassifierModel;
import de.dfki.km.perspecting.obie.transducer.model.disambiguation.AmbiguityResolver;
import de.dfki.km.perspecting.obie.transducer.model.disambiguation.FlowBasedResolver;
import de.dfki.km.perspecting.obie.transducer.model.rating.CapacityBasedRating;
import de.dfki.km.perspecting.obie.transducer.model.rating.RatingMetric;
import de.dfki.km.perspecting.obie.vocabulary.Language;
import de.dfki.km.perspecting.obie.vocabulary.MediaType;
import de.dfki.km.perspecting.obie.workflow.Pipeline;

public class QueryExperiment {

	/**
		 * 
		 */

	private static PoolingDataSource pool = new PoolingDataSource();

	private static Pipeline pipeline;

	/******************* technical setup ******************************************/

	private static String $PHD_HOME = "/home/adrian/Dokumente/diss/";
	private static String $SCOOBIE_HOME = $PHD_HOME + "scoobie/";
	private static String $CORPUS_HOME = $PHD_HOME + "textcorpus/";

	private static String $DATABASE_DBPEDIA_en2 = "dbpedia_en2";
	private static String $DATABASE_BBC_MUSIC = "bbc_music";
	private static String $DATABASE_BBC_WILDLIFE = "bbc_wildlife";

	private static String $DATABASE_SERVER_PC_4327 = "pc-4327.kl.dfki.de";
	private static String $DATABASE_SERVER_LOCALHOST = "localhost";

	private static String $DATABASE_SERVER_USER = "postgres";
	private static String $DATABASE_SERVER_PW = "scoobie";
	private static int $DATABASE_SERVER_PORT = 5432;

	final String RDFS_LABEL = "http://www.w3.org/2000/01/rdf-schema#label";
	final String DC_TITLE = "http://purl.org/dc/terms/title";
	final String DC_TITLE_ELEMENTS = "http://purl.org/dc/elements/1.1/title";
	final String WO_SPECIESNAME = "http://purl.org/ontology/wo/speciesName";
	final String FOAF_NAME = "http://xmlns.com/foaf/0.1/name";
	final String OV_SORTLABEL = "http://open.vocab.org/terms/sortLabel";
	final String DBONT_REVIEW = "http://dbpedia.org/ontology/review";

	private static KnowledgeBase kb;

	/**
	 * @throws java.lang.Exception
	 */

	public static void setUp(String $DATABASE, String $DATABASE_SERVER,
			String classificationModel, TextCorpus corpus) throws Exception {

		pool.setUser($DATABASE_SERVER_USER);
		pool.setPassword($DATABASE_SERVER_PW);
		pool.setPortNumber($DATABASE_SERVER_PORT);
		pool.setDatabaseName($DATABASE);
		pool.setServerName($DATABASE_SERVER);
		pool.setMaxConnections(100);

		kb = new PostgresKB(pool.getConnection(), $DATABASE, new URI("http://dbpedia.org"));
		pipeline = new Pipeline(kb);

		LanguageIdentification languageClassification = new LanguageIdentification(
				Language.EN);
		WordSegmenter wordTokenizer = new WordSegmenter();
		SentenceSegmenter sentenceTokenizer = new SentenceSegmenter();


		POSModel posModel = new POSModel(Scoobie.class.getResourceAsStream("pos/en/en-pos-maxent.bin"));
		POSTagging posTagger = new POSTagging(new POSTaggerME(posModel));

		ProperNameRecognition nounPhraseChunker = new ProperNameRecognition(new CRFNounPhraseChunkerModel(Scoobie.class.getResourceAsStream("npc/en/EN.crf")));

		
		SuffixArrayBuilder suffixArrayBuilder = new SuffixArrayBuilder(100, new LiteralHashing(4));
		RDFLiteralSpotting namedEntityRecognizer = new RDFLiteralSpotting();
		InstanceRecognition instanceResolver = new InstanceRecognition();
		EntityDisambiguation instanceDisambiguator = new EntityDisambiguation(
				new AmbiguityResolver[] { new FlowBasedResolver() });
		KnownFactsRetrieval factRetrieval = new KnownFactsRetrieval();
		FactRecommender factRecommender = new FactRecommender();
		
		
		RelevanceRating relevanceRating = new RelevanceRating(
				new RatingMetric[] {
						new CapacityBasedRating()}
				);
		
		MaxentEntityClassifierModel cl = new MaxentEntityClassifierModel(
				classificationModel, Language.EN, new int[] { 1, 2, 3 }, true,
				true, true, 1.0, 4, new String[] { "VB", "ADJ", "NNP", "NN",
						"NNS" });
		cl.load(kb, null);
		EntityClassification namedEntityClassifier = new EntityClassification(
				0.6, cl.getClassifier());
		
		
		String DATE = "(19|20)\\\\d{2}-(0[1-9]|1[012]|[1-9])-(0[1-9]|[1-9]|[12][0-9]|3[01])";
		String MAIL = "[\\\\w]+@[\\\\w]+";
		String ISBN10 = "ISBN\\\\x20(?=.{13}$)\\\\d{1,5}([- ])\\\\d{1,7}\\\\1\\\\d{1,6}\\\\1(\\\\d|X)$";
		String FLOAT = "[-]?[0-9]+\\\\.[0-9]+";
		String POINT = "[-]?[0-9]+\\\\.[0-9]+ [-]?[0-9]+\\\\.[0-9]+";
		String[] patterns = new String[] { DATE, FLOAT, POINT };
		kb.calculateRegexDistributions(patterns);
		RegularStructuredEntityRecognition structuredEntityRecognizer = new RegularStructuredEntityRecognition(patterns);
		

		
		
		pipeline.configure(languageClassification,
				wordTokenizer, sentenceTokenizer, posTagger, nounPhraseChunker,
				suffixArrayBuilder, namedEntityRecognizer, structuredEntityRecognizer,
				namedEntityClassifier, instanceResolver, instanceDisambiguator,
				factRetrieval, relevanceRating, factRecommender);

	}

	public static IndexSearcher index(TextCorpus corpus, String path)
			throws Exception {

		// if (new File(path).exists()) {
		// FileUtils.deleteDirectory(new File(path));
		// System.out.println("deleted : " + path);
		// }

//		new File(path).mkdirs();
//
//		final WhitespaceAnalyzer analyser = new WhitespaceAnalyzer();
//		final IndexWriter indexWriter = new IndexWriter(path, analyser, true,
//				MaxFieldLength.LIMITED);
//		corpus.forEach(new DocumentProcedure<String>() {
//			@Override
//			public String process(Reader doc, String uri) throws Exception {
//				org.apache.lucene.document.Document document = new org.apache.lucene.document.Document();
//				document.add(new Field("text", doc, TermVector.YES));
//				indexWriter.addDocument(document, analyser);
//				System.out.print("#");
//				return uri;
//			}
//		});
//		System.out.println();
//		System.out.println("indexed: " + indexWriter.numDocs() + " documents");
//
//		indexWriter.commit();
//		indexWriter.close();

		return new IndexSearcher(path);
	}

	@AfterClass
	public static void shutdownAfterClass() {
		pool.close();
	}

	@Test
	public void testQuery() throws Exception {
		final File wikinewsFolder = new File($CORPUS_HOME + "en/wikinews/");
		final WikinewsCorpus corpus = new WikinewsCorpus();
		setUp($DATABASE_DBPEDIA_en2, $DATABASE_SERVER_PC_4327, $CORPUS_HOME
				+ "en/wikipedia.type.maxent", corpus);
		
		String query = 
		    "PREFIX scoobie: <http://scoobie.opendfki.de/vocabulary#>\n" + 
			"PREFIX dbp-ont: <http://dbpedia.org/ontology/>\n" + 
			"PREFIX rdf:    <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" + 
			"PREFIX rdfs:    <http://www.w3.org/2000/01/rdf-schema#>\n" + 
			"PREFIX dc:      <http://purl.org/dc/elements/1.1/>\n" + 
			"\n" + 
			"SELECT *\n" + 
			"  FROM          <http://spiegel.de>    # source document\n" + 
			"  FROM NAMED    <http://dbpedia.org>   # RDF graph\n" + 
			"  FROM NAMED    scoobie:predicted      # predicted triples\n" + 
			"  FROM NAMED    scoobie:recognized     # recognized triples\n" + 
			"WHERE {\n" + 
			"   ?doc dc:title ?title ;\n" + 
			"        dc:published ?date .\n" + 
			"   GRAPH scoobie:predicted {\n" + 
			"      ?s1 rdfs:label ?name1 .\n" + 
			"    }\n" + 
			"   GRAPH scoobie:recognized {   \n" + 
			"      ?s2 rdfs:label ?name2 ; a dbp-ont:Person; dbp-ont:birthPlace ?city \n" + 
			"   }\n" + 
			"   GRAPH <http://dbpedia.org> {   \n" + 
			"      ?s2 rdf:type dbp-ont:Person .\n" + 
			"   }\n" + 
			"}";
		
		String query2 = 
		    "PREFIX scoobie: <http://scoobie.opendfki.de/vocabulary#>\n" + 
			"PREFIX dbp-ont: <http://dbpedia.org/ontology/>\n" + 
			"PREFIX rdf:    <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" + 
			"PREFIX rdfs:    <http://www.w3.org/2000/01/rdf-schema#>\n" + 
			"PREFIX dc:      <http://purl.org/dc/elements/1.1/>\n" + 
			"\n" + 
			"SELECT *\n" + 
			"  FROM          <http://spiegel.de>    # source document\n" + 
			"  FROM NAMED    <http://dbpedia.org>   # RDF graph\n" + 
			"  FROM NAMED    scoobie:predicted      # predicted triples\n" + 
			"  FROM NAMED    scoobie:recognized     # recognized triples\n" + 
			"WHERE {\n" + 
			"   GRAPH scoobie:recognized {   \n" + 
			"      ?s2 a dbp-ont:Person \n" + 
			"   }\n" + 
			"   GRAPH <http://dbpedia.org> {   \n" + 
			"      ?s2 a dbp-ont:Person .\n" + 
			"   }\n" + 
			"}";
		
		String query3 = 
		    "PREFIX scoobie: <http://scoobie.opendfki.de/vocabulary#>\n" + 
			"PREFIX dbp-ont: <http://dbpedia.org/ontology/>\n" + 
			"PREFIX rdf:    <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" + 
			"PREFIX rdfs:    <http://www.w3.org/2000/01/rdf-schema#>\n" + 
			"PREFIX dc:      <http://purl.org/dc/elements/1.1/>\n" + 
			"\n" + 
			"SELECT *\n" + 
			"  FROM          <http://spiegel.de>    # source document\n" + 
			"  FROM NAMED    <http://dbpedia.org>   # RDF graph\n" + 
			"  FROM NAMED    scoobie:predicted      # predicted triples\n" + 
			"  FROM NAMED    scoobie:recognized     # recognized triples\n" + 
			"WHERE {\n" + 
			"   GRAPH scoobie:recognized {   \n" + 
			"      ?s dbp-ont:birthPlace ?o .\n" + 
			"   }\n" + 
			"   GRAPH <http://dbpedia.org> {   \n" + 
			"      ?s dbp-ont:birthPlace ?o .\n" + 
			"   }\n" + 
			"}";
		
		
		FilterContext template = new FilterContext(new URI("http://dbpedia.org"), "http://scoobie.opendfki.de/vocabulary#predicted", "http://scoobie.opendfki.de/vocabulary#recognized",kb, query3);
		template.infer(0.001, 0.5);
		System.out.println(template.getContextGraphs());
		System.out.println(template.getNamedGraphs());
		System.out.println(template.getDefaultGraphs());
		
		System.out.println(template.getSubjectFilter());
		System.out.println(template.getDatatypePropertyFilter());
		System.out.println(template.getObjectPropertyFilter());
		System.out.println(template.getTypeFilter());
	}
	
	@Test
	public void test() throws Exception {
		
		final File wikinewsFolder = new File($CORPUS_HOME + "en/wikinews/");
		final WikinewsCorpus corpus = new WikinewsCorpus();
		
		setUp($DATABASE_DBPEDIA_en2, $DATABASE_SERVER_PC_4327, $CORPUS_HOME
				+ "en/wikipedia.type.maxent", corpus);
		final String template = "SELECT * WHERE {?s <" + RDFS_LABEL + "> ?o}";
		
		String text = "In Berlin, Angela Merkel, Guido Westerwelle, Helmut Kohl, and Harald Schmitt spoke about Germany.";
		String text1 = "Angela Merkel visited Kaiserslautern and Otterbach and Zweibrücken";
		String text2 = "In Kaiserslautern,  2001-06-22 Freddie Mercury played for Queen the song We Will Rock You together with Brian May, Roger Taylor, and Frank Zappa.";
		
		Document document = pipeline.createDocument(text2, new URI("/bla"), MediaType.TEXT, template, Language.EN);
		for (int step = 0; pipeline.hasNext(step); step = pipeline.execute(step, document));
		
//		BufferedReader reader = new BufferedReader(new RDFSerializer("http://this.document").serialize(document, kb));
//		for (String line = reader.readLine(); line != null; line = reader.readLine()) {
//			System.out.println(line);
//		}
		BufferedReader reader2 = new BufferedReader(new ListSerializer(0).serialize(document, kb));
		for (String line = reader2.readLine(); line != null; line = reader2.readLine()) {
			System.out.println(line);
		}
	}
	

}

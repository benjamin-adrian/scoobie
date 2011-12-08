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
import java.io.FileWriter;
import java.io.Reader;
import java.net.URI;

import opennlp.tools.postag.POSModel;
import opennlp.tools.postag.POSTaggerME;

import org.junit.AfterClass;
import org.junit.Test;
import org.postgresql.jdbc2.optional.PoolingDataSource;

import cern.colt.function.IntIntDoubleFunction;
import cern.colt.matrix.doublealgo.Statistic;
import cern.colt.matrix.linalg.Algebra;
import de.dfki.km.perspecting.obie.connection.KnowledgeBase;
import de.dfki.km.perspecting.obie.connection.PostgresKB;
import de.dfki.km.perspecting.obie.corpus.BBCMusicCorpus;
import de.dfki.km.perspecting.obie.corpus.BBCNatureCorpus;
import de.dfki.km.perspecting.obie.corpus.GutenbergCorpus;
import de.dfki.km.perspecting.obie.corpus.WikinewsCorpus;
import de.dfki.km.perspecting.obie.corpus.WikipediaCorpus;
import de.dfki.km.perspecting.obie.model.Document;
import de.dfki.km.perspecting.obie.model.DocumentProcedure;
import de.dfki.km.perspecting.obie.model.DoubleMatrix;
import de.dfki.km.perspecting.obie.model.Scoobie;
import de.dfki.km.perspecting.obie.postprocessor.RDFSerializer;
import de.dfki.km.perspecting.obie.transducer.EntityDisambiguation;
import de.dfki.km.perspecting.obie.transducer.FactRecommender;
import de.dfki.km.perspecting.obie.transducer.InstanceRecognition;
import de.dfki.km.perspecting.obie.transducer.KnownFactsRetrieval;
import de.dfki.km.perspecting.obie.transducer.LanguageIdentification;
import de.dfki.km.perspecting.obie.transducer.POSTagging;
import de.dfki.km.perspecting.obie.transducer.ProperNameRecognition;
import de.dfki.km.perspecting.obie.transducer.RDFLiteralSpotting;
import de.dfki.km.perspecting.obie.transducer.SentenceSegmenter;
import de.dfki.km.perspecting.obie.transducer.SuffixArrayBuilder;
import de.dfki.km.perspecting.obie.transducer.WordSegmenter;
import de.dfki.km.perspecting.obie.transducer.model.CRFNounPhraseChunkerModel;
import de.dfki.km.perspecting.obie.transducer.model.disambiguation.AmbiguityResolver;
import de.dfki.km.perspecting.obie.transducer.model.disambiguation.DegreeBasedResolver;
import de.dfki.km.perspecting.obie.vocabulary.Language;
import de.dfki.km.perspecting.obie.vocabulary.MediaType;
import de.dfki.km.perspecting.obie.workflow.DummyTask;
import de.dfki.km.perspecting.obie.workflow.Evaluator;
import de.dfki.km.perspecting.obie.workflow.Pipeline;

public class FactRecommendationExperiment {

	/**
		 * 
		 */

	private static PoolingDataSource pool = new PoolingDataSource();

	private static Pipeline pipeline;

	/******************* technical setup ******************************************/

	private static String $PHD_HOME = "/home/adrian/Dokumente/diss/";
	private static String $SCOOBIE_HOME = $PHD_HOME + "scoobie/";

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

	public static void setUp(String $DATABASE, String $DATABASE_SERVER) throws Exception {

		pool.setUser($DATABASE_SERVER_USER);
		pool.setPassword($DATABASE_SERVER_PW);
		pool.setPortNumber($DATABASE_SERVER_PORT);
		pool.setDatabaseName($DATABASE);
		pool.setServerName($DATABASE_SERVER);
		pool.setMaxConnections(100);

		kb = new PostgresKB(pool.getConnection(), $DATABASE, new URI("http://test.de"));
		pipeline = new Pipeline(kb);


		LanguageIdentification languageClassification = new LanguageIdentification(
				Language.EN);
		WordSegmenter wordTokenizer = new WordSegmenter();
		SentenceSegmenter sentenceTokenizer = new SentenceSegmenter();

		POSModel posModel = new POSModel(
				Scoobie.class.getResourceAsStream("pos/en/en-pos-maxent.bin"));
		POSTagging posTagger = new POSTagging(new POSTaggerME(posModel));
		ProperNameRecognition nounPhraseChunker = new ProperNameRecognition(
				new CRFNounPhraseChunkerModel(
						Scoobie.class.getResourceAsStream("npc/en/EN.crf")));

		SuffixArrayBuilder suffixArrayBuilder = new SuffixArrayBuilder(100);
		RDFLiteralSpotting entityRecognizer = new RDFLiteralSpotting();
		InstanceRecognition subjectResolver = new InstanceRecognition();
		EntityDisambiguation subjectDisambiguator = new EntityDisambiguation(
				new AmbiguityResolver[] { new DegreeBasedResolver() });
		KnownFactsRetrieval factRetrieval = new KnownFactsRetrieval();
		FactRecommender factRecommender = new FactRecommender();

		pipeline.configure(languageClassification,
				wordTokenizer, sentenceTokenizer, posTagger, nounPhraseChunker,
				suffixArrayBuilder, entityRecognizer, new DummyTask(),
				new DummyTask(), subjectResolver, subjectDisambiguator,
				factRetrieval, new DummyTask(), factRecommender);

	}

	@AfterClass
	public static void shutdownAfterClass() {
		pool.close();
	}

	@Test
	public void testSmall() throws Exception {
		setUp($DATABASE_DBPEDIA_en2, $DATABASE_SERVER_PC_4327);
		final String template = "SELECT * WHERE {?s <" + RDFS_LABEL + "> ?o}";
		String text = "In Berlin, Angela Merkel, Guido Westerwelle, Helmut Kohl, and Harald Schmitt spoke about Germany.";
		String text1 = "Angela Merkel visited Kaiserslautern and Otterbach and Zweibrücken";
		String text2 = "Freddie Mercury played for Queen the song We Will Rock You together with Brian May, Roger Taylor, and Frank Zappa.";
		Document document = pipeline.createDocument(text2, new URI("/bla"), MediaType.TEXT, template, Language.EN);

		for (int step = 0; pipeline.hasNext(step); step = pipeline.execute(
				step, document)) {

		}

		BufferedReader r = new BufferedReader(new RDFSerializer(
				"http://this.document").serialize(document, kb));
		for (String line = r.readLine(); line != null; line = r.readLine()) {
			System.out.println(line);
		}
		
	}

	@Test
	public void testSmallBBCMusic() throws Exception {
		setUp($DATABASE_BBC_MUSIC, $DATABASE_SERVER_LOCALHOST);
		final String template = "SELECT * WHERE {?s ?p ?o}";
		String text = "Freddie Mercury played for Queen together with Brian May, Roger Taylor, and Frank Zappa.";
		
		Document document = pipeline.createDocument(text, new URI("/bla"), MediaType.TEXT, template, Language.EN);

		for (int step = 0; pipeline.hasNext(step); step = pipeline.execute(
				step, document)) {

		}

		BufferedReader r = new BufferedReader(new RDFSerializer(
				"http://this.document").serialize(document, kb));
		for (String line = r.readLine(); line != null; line = r.readLine()) {
			System.out.println(line);
		}
	}

	@Test
	public void createCardinalities() throws Exception {
		setUp($DATABASE_DBPEDIA_en2, $DATABASE_SERVER_PC_4327);
		
		kb.calculateCardinalities();
		
	}
	
	@Test
	public void createCardinalitiesBBCNature() throws Exception {
		setUp($DATABASE_BBC_WILDLIFE, $DATABASE_SERVER_LOCALHOST);
		kb.calculateCardinalities();
	}
	
	@Test
	public void createCardinalitiesBBCMusic() throws Exception {
		setUp($DATABASE_BBC_MUSIC, $DATABASE_SERVER_LOCALHOST);
		kb.calculateCardinalities();
	}
	
	@Test
	public void createMarkovChain() throws Exception {
		setUp($DATABASE_DBPEDIA_en2, $DATABASE_SERVER_PC_4327);
		kb.calculateMarkovChain(new int[] {}, 100);
	}
	
	@Test
	public void createMarkovChainBBCNature() throws Exception {
		setUp($DATABASE_BBC_WILDLIFE, $DATABASE_SERVER_LOCALHOST);
		kb.calculateMarkovChain(new int[] {}, 100);
	}

	@Test
	public void createMarkovChainBBCMusic() throws Exception {
		setUp($DATABASE_BBC_MUSIC, $DATABASE_SERVER_LOCALHOST);
		kb.calculateMarkovChain(new int[] {}, 100);
	}

	/**
	 * Test method for
	 * {@link de.dfki.km.perspecting.obie.dixi.service.SimpleScobieService#extractInformationFromURL(java.lang.String, java.lang.String)}
	 * .
	 */
	@Test
	public void testWikinewsCorpus() {

		try {
			setUp($DATABASE_DBPEDIA_en2, $DATABASE_SERVER_PC_4327);
			final String template = "SELECT * WHERE {?s <" + RDFS_LABEL
					+ "> ?o}";

			final WikinewsCorpus corpus = new WikinewsCorpus();

			final FileWriter w = new FileWriter($SCOOBIE_HOME + "results/"
					+ $DATABASE_DBPEDIA_en2 + "/wikinews_recommender_cos.csv");

			corpus.forEach(new DocumentProcedure<URI>() {
				@Override
				public URI process(Reader doc, URI uri) throws Exception {
					Document document = pipeline.createDocument(doc, uri,corpus.getMediatype(), 
							template, Language.EN);

					Evaluator evaluator = new Evaluator(pipeline);
					for (int step = 0; pipeline.hasNext(step); step = pipeline
							.execute(step, document)) {
						
						if (step == 13)
							evaluator.evaluate(13, document, corpus, w);

					}
					return uri;

				}
			});
			w.close();

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/**
	 * Test method for
	 * {@link de.dfki.km.perspecting.obie.dixi.service.SimpleScobieService#extractInformationFromURL(java.lang.String, java.lang.String)}
	 * .
	 */
	@Test
	public void testGutenbergCorpus() {

		try {
			setUp($DATABASE_DBPEDIA_en2, $DATABASE_SERVER_PC_4327);
			final String template = "SELECT * WHERE {?s <" + RDFS_LABEL
					+ "> ?o}";

			final GutenbergCorpus corpus = new GutenbergCorpus();

			final FileWriter w = new FileWriter($SCOOBIE_HOME + "results/"
					+ $DATABASE_DBPEDIA_en2 + "/gutenberg_disambiguation.csv");

			corpus.forEach(new DocumentProcedure<URI>() {
				@Override
				public URI process(Reader doc, URI uri) throws Exception {
					Document document = pipeline.createDocument(doc, uri,corpus.getMediatype(), 
							template, Language.EN);

					Evaluator evaluator = new Evaluator(pipeline);
					for (int step = 0; pipeline.hasNext(step); step = pipeline
							.execute(step, document)) {
						
						if (step == 13)
							evaluator.evaluate(13, document, corpus, w);

					}
					return uri;

				}
			});
			w.close();

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/**
	 * Test method for
	 * {@link de.dfki.km.perspecting.obie.dixi.service.SimpleScobieService#extractInformationFromURL(java.lang.String, java.lang.String)}
	 * .
	 */
	@Test
	public void testWikipediaCorpus() {

		try {
			setUp($DATABASE_DBPEDIA_en2, $DATABASE_SERVER_PC_4327);
			final String template = "SELECT * WHERE {?s <" + RDFS_LABEL
					+ "> ?o}";


			final WikipediaCorpus corpus = new WikipediaCorpus();

			final FileWriter w = new FileWriter($SCOOBIE_HOME + "results/"
					+ $DATABASE_DBPEDIA_en2 + "/wikipedia_disambiguation.csv");

			corpus.forEach(new DocumentProcedure<URI>() {
				@Override
				public URI process(Reader doc, URI uri) throws Exception {
					Document document = pipeline.createDocument(doc, uri,corpus.getMediatype(), 
							template, Language.EN);

					Evaluator evaluator = new Evaluator(pipeline);
					for (int step = 0; pipeline.hasNext(step); step = pipeline
							.execute(step, document)) {
						
						if (step == 13)
							evaluator.evaluate(13, document, corpus, w);

					}
					return uri;

				}
			});
			w.close();

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	@Test
	public void testBBCMusicCorpus() {
		try {
			setUp($DATABASE_BBC_MUSIC, $DATABASE_SERVER_LOCALHOST);

			final String template = "SELECT * WHERE {?s <" + OV_SORTLABEL
					+ "> ?o . ?s1 <" + FOAF_NAME + "> ?o1. ?s2 <" + DC_TITLE
					+ "> ?o2}";

			final BBCMusicCorpus corpus = new BBCMusicCorpus();

			final FileWriter w = new FileWriter($SCOOBIE_HOME + "results/"
					+ $DATABASE_BBC_MUSIC + "/fact_recommender_colf1.csv");

			corpus.forEach(new DocumentProcedure<URI>() {
				@Override
				public URI process(Reader doc, URI uri) throws Exception {
					Document document = pipeline.createDocument(doc, uri,corpus.getMediatype(), 
							template, Language.EN);

					Evaluator evaluator = new Evaluator(pipeline);
					for (int step = 0; pipeline.hasNext(step); step = pipeline
							.execute(step, document)) {
						
						if (step == 13)
							evaluator.evaluate(13, document, corpus, w);

					}
					return uri;

				}
			});
			w.close();

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	@Test
	public void testBBCWildlifeCorpus() {

		try {
			setUp($DATABASE_BBC_WILDLIFE, $DATABASE_SERVER_LOCALHOST);

			final String template = "SELECT * WHERE {?s <http://www.w3.org/2000/01/rdf-schema#label> ?o . ?s1 <http://purl.org/dc/terms/title> ?o1 }";
			final BBCNatureCorpus corpus = new BBCNatureCorpus();

			final FileWriter w = new FileWriter($SCOOBIE_HOME + "results/"
					+ $DATABASE_BBC_WILDLIFE + "/recommend_facts1.csv");

			corpus.forEach(new DocumentProcedure<URI>() {
				@Override
				public URI process(Reader doc, URI uri) throws Exception {
					Document document = pipeline.createDocument(doc, uri,corpus.getMediatype(), 
							template, Language.EN);

					Evaluator evaluator = new Evaluator(pipeline);
					for (int step = 0; pipeline.hasNext(step); step = pipeline
							.execute(step, document)) {
						
						if (step == 13)
							evaluator.evaluate(13, document, corpus, w);

					}
					return uri;

				}
			});
			w.close();

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	
	@Test
	public void testMatrix() {
		
		DoubleMatrix m = new DoubleMatrix();
		m.add(0, 0, 1);
		m.add(0, 1, 0);
		m.add(0, 2, 0);
		m.add(0, 3, 1);
		m.add(1, 0, 0);
		m.add(1, 1, 0);
		m.add(1, 2, 1);
		m.add(1, 3, 0);
		m.add(2, 0, 0);
		m.add(2, 1, 1);
		m.add(2, 2, 0);
		m.add(2, 3, 0);
		m.add(3, 0, 1);
		m.add(3, 1, 0);
		m.add(3, 2, 0);
		m.add(3, 3, 0);
		m.add(4, 0, 1);
		m.add(4, 1, 0);
		m.add(4, 2, 0);
		m.add(4, 3, 0);
		m.add(5, 0, 0);
		m.add(5, 1, 1);
		m.add(5, 2, 0);
		m.add(5, 3, 0);
		m.add(6, 0, 1);
		m.add(6, 1, 1);
		m.add(6, 2, 0);
		m.add(6, 3, 0);
		
//		System.out.println(m.toColt());
		System.out.println(m.cosineSimilarity(0,1,2));
		System.out.println(m.cosineSimilarity());
		System.out.println(Statistic.correlation(Statistic.covariance(Algebra.DEFAULT.transpose(m.toColt()))).forEachNonZero(new IntIntDoubleFunction() {
			
			@Override
			public double apply(int arg0, int arg1, double arg2) {
				return arg2 < 0 ? 0 : arg2;
			}
		}));
		
		System.out.println(m.predictValuesByCosine(Statistic.correlation(Statistic.covariance(Algebra.DEFAULT.transpose(m.toColt()))).forEachNonZero(new IntIntDoubleFunction() {
			
			@Override
			public double apply(int arg0, int arg1, double arg2) {
				return arg2 < 0 ? 0 : arg2;
			}
		}), m.toColt()));
		System.out.println(m.predictValuesByCosine(m.cosineSimilarity(0, 1, 2), m.toColt()));
		System.out.println(m.predictValuesByCosine(m.cosineSimilarity(), m.toColt()));
	}
	
}

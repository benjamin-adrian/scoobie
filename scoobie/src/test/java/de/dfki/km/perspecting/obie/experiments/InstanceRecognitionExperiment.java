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

import java.io.FileWriter;
import java.io.Reader;
import java.net.URI;

import opennlp.tools.postag.POSModel;
import opennlp.tools.postag.POSTaggerME;

import org.junit.AfterClass;
import org.junit.Test;
import org.postgresql.jdbc2.optional.PoolingDataSource;

import de.dfki.km.perspecting.obie.connection.KnowledgeBase;
import de.dfki.km.perspecting.obie.connection.PostgresKB;
import de.dfki.km.perspecting.obie.corpus.BBCMusicCorpus;
import de.dfki.km.perspecting.obie.corpus.BBCNatureCorpus;
import de.dfki.km.perspecting.obie.corpus.GutenbergCorpus;
import de.dfki.km.perspecting.obie.corpus.WikinewsCorpus;
import de.dfki.km.perspecting.obie.corpus.WikipediaCorpus;
import de.dfki.km.perspecting.obie.model.Document;
import de.dfki.km.perspecting.obie.model.DocumentProcedure;
import de.dfki.km.perspecting.obie.model.Scoobie;
import de.dfki.km.perspecting.obie.transducer.InstanceRecognition;
import de.dfki.km.perspecting.obie.transducer.LanguageIdentification;
import de.dfki.km.perspecting.obie.transducer.POSTagging;
import de.dfki.km.perspecting.obie.transducer.ProperNameRecognition;
import de.dfki.km.perspecting.obie.transducer.RDFLiteralSpotting;
import de.dfki.km.perspecting.obie.transducer.SentenceSegmenter;
import de.dfki.km.perspecting.obie.transducer.SuffixArrayBuilder;
import de.dfki.km.perspecting.obie.transducer.WordSegmenter;
import de.dfki.km.perspecting.obie.transducer.model.CRFNounPhraseChunkerModel;
import de.dfki.km.perspecting.obie.vocabulary.Language;
import de.dfki.km.perspecting.obie.vocabulary.MediaType;
import de.dfki.km.perspecting.obie.workflow.DummyTask;
import de.dfki.km.perspecting.obie.workflow.Evaluator;
import de.dfki.km.perspecting.obie.workflow.Pipeline;

public class InstanceRecognitionExperiment {

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

	private static KnowledgeBase kb;


	@AfterClass
	public static void shutdownAfterClass() {
		pool.close();
	}

	/**
	 * Test method for
	 * {@link de.dfki.km.perspecting.obie.dixi.service.SimpleScobieService#extractInformationFromURL(java.lang.String, java.lang.String)}
	 * .
	 */
	@Test
	public void testWikinewsCorpus() {
		try {

			initSCOOBIE($DATABASE_SERVER_PC_4327, $DATABASE_DBPEDIA_en2);

			final String template = "SELECT * WHERE {?s ?p ?o. }";
			final String template1 = "SELECT * WHERE {?s <http://www.w3.org/2000/01/rdf-schema#label> ?o. }";
			final String template2 = "SELECT * WHERE {?s <http://www.w3.org/2000/01/rdf-schema#label> ?o. ?s1 <"
					+ FOAF_NAME + "> ?o1}";
			final String template3 = "SELECT * WHERE {?s <http://www.w3.org/2000/01/rdf-schema#label> ?o. ?s1 <"
					+ FOAF_NAME + "> ?o1. ?s2 <" + DBONT_REVIEW + "> ?o2}";

			final WikinewsCorpus corpus = new WikinewsCorpus();

			final FileWriter w = new FileWriter($SCOOBIE_HOME + "results/"
					+ $DATABASE_DBPEDIA_en2
					+ "/wikinews_instance_recognition_rating_1.csv");

			corpus.forEach(new DocumentProcedure<URI>() {
				@Override
				public URI process(Reader doc, URI uri) throws Exception {
					Document document = pipeline.createDocument(doc, uri, MediaType.HTML, template1, Language.EN);

					Evaluator evaluator = new Evaluator(pipeline);

					for (int step = 0; pipeline.hasNext(step); step = pipeline
							.execute(step, document)) {

						if (step == 10) {
							evaluator.evaluate(9, document, corpus, w);
						}
					}

					return uri;
				}
			});
			w.close();

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Test
	public void testGutenbergCorpus() {
		try {

			initSCOOBIE($DATABASE_SERVER_PC_4327, $DATABASE_DBPEDIA_en2);

			final String template = "SELECT * WHERE {?s ?p ?o. }";
			final String template1 = "SELECT * WHERE {?s <http://www.w3.org/2000/01/rdf-schema#label> ?o. }";
			final String template2 = "SELECT * WHERE {?s <http://www.w3.org/2000/01/rdf-schema#label> ?o. ?s1 <"
					+ FOAF_NAME + "> ?o1}";

			final GutenbergCorpus corpus = new GutenbergCorpus();

			final FileWriter w = new FileWriter($SCOOBIE_HOME + "results/"
					+ $DATABASE_DBPEDIA_en2
					+ "/gutenberg_instance_recognition_rating_2.csv");

			corpus.forEach(new DocumentProcedure<URI>() {
				@Override
				public URI process(Reader doc, URI uri) throws Exception {
					Document document = pipeline.createDocument(doc, uri, MediaType.HTML, template1, Language.EN);

					Evaluator evaluator = new Evaluator(pipeline);

					for (int step = 0; pipeline.hasNext(step); step = pipeline
							.execute(step, document)) {

						if (step == 10) {
							evaluator.evaluate(9, document, corpus, w);
						}
					}

					return uri;
				}
			});
			w.close();

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * @param $database2
	 * @param $databaseSERVER
	 * @throws Exception
	 */
	private void initSCOOBIE(String $databaseSERVER, String $database2)
			throws Exception {
		pool.setUser($DATABASE_SERVER_USER);
		pool.setPassword($DATABASE_SERVER_PW);
		pool.setPortNumber($DATABASE_SERVER_PORT);
		pool.setDatabaseName($database2);
		pool.setServerName($databaseSERVER);
		pool.setMaxConnections(100);

		kb = new PostgresKB(pool.getConnection(), $database2, new URI("http://test.de"));
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
		entityRecognizer.setFilterLongestMatch(true);
		InstanceRecognition subjectResolver = new InstanceRecognition();
		subjectResolver.setCollabseSymbols(false);

		pipeline.configure(languageClassification,
				wordTokenizer, sentenceTokenizer, posTagger, nounPhraseChunker,
				suffixArrayBuilder, entityRecognizer, new DummyTask(),
				new DummyTask(), subjectResolver, new DummyTask(),new DummyTask(),
				new DummyTask(), new DummyTask());
	}

	@Test
	public void testBBCMusicCorpus() {

		try {

			initSCOOBIE($DATABASE_SERVER_LOCALHOST, $DATABASE_BBC_MUSIC);

			final String template = "SELECT * WHERE {?s ?p ?o. }";
			final String template1 = "SELECT * WHERE {?s <" + OV_SORTLABEL
					+ "> ?o }";
			final String template2 = "SELECT * WHERE {?s <" + OV_SORTLABEL
					+ "> ?o . ?s1 <" + FOAF_NAME + "> ?o1 }";
			final String template3 = "SELECT * WHERE {?s <" + OV_SORTLABEL
					+ "> ?o . ?s1 <" + FOAF_NAME + "> ?o1. ?s2 <" + DC_TITLE
					+ "> ?o2}";

			final BBCMusicCorpus corpus = new BBCMusicCorpus();

			final FileWriter w = new FileWriter($SCOOBIE_HOME + "results/"
					+ $DATABASE_BBC_MUSIC
					+ "/instance_recognition_rating_3.csv");

			corpus.forEach(new DocumentProcedure<URI>() {
				@Override
				public URI process(Reader doc, URI uri) throws Exception {
					Document document = pipeline.createDocument(doc, uri, MediaType.HTML, template1, Language.EN);

					Evaluator evaluator = new Evaluator(pipeline);

					for (int step = 0; pipeline.hasNext(step); step = pipeline
							.execute(step, document)) {

						if (step == 10) {
							evaluator.evaluate(9, document, corpus, w);
						}
					}

					return uri;
				}
			});
			w.close();

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	final String RDFS_LABEL = "http://www.w3.org/2000/01/rdf-schema#label";
	final String DC_TITLE = "http://purl.org/dc/terms/title";
	final String DC_TITLE_ELEMENTS = "http://purl.org/dc/elements/1.1/title";
	final String WO_SPECIESNAME = "http://purl.org/ontology/wo/speciesName";
	final String FOAF_NAME = "http://xmlns.com/foaf/0.1/name";
	final String OV_SORTLABEL = "http://open.vocab.org/terms/sortLabel";
	final String DBONT_REVIEW = "http://dbpedia.org/ontology/review";

	@Test
	public void testBBCWildlifeCorpus() {

		try {

			initSCOOBIE($DATABASE_SERVER_LOCALHOST, $DATABASE_BBC_WILDLIFE);

			final String template1 = "SELECT * WHERE {?s <http://www.w3.org/2000/01/rdf-schema#label> ?o }";
			final String template2 = "SELECT * WHERE {?s <http://www.w3.org/2000/01/rdf-schema#label> ?o . ?s1 <http://purl.org/dc/terms/title> ?o1 }";
			final String template3 = "SELECT * WHERE {?s <" + RDFS_LABEL
					+ "> ?o . ?s1 <" + DC_TITLE + "> ?o1 . ?s2 <"
					+ WO_SPECIESNAME + "> ?o2}";

			final BBCNatureCorpus corpus = new BBCNatureCorpus();

			final FileWriter w = new FileWriter($SCOOBIE_HOME + "results/"
					+ $DATABASE_BBC_WILDLIFE
					+ "/instance_recognition_rating_3.csv");

			corpus.forEach(new DocumentProcedure<URI>() {
				@Override
				public URI process(Reader doc, URI uri) throws Exception {
					Document document = pipeline.createDocument(doc, uri, MediaType.HTML, template1, Language.EN);

					Evaluator evaluator = new Evaluator(pipeline);

					for (int step = 0; pipeline.hasNext(step); step = pipeline
							.execute(step, document)) {

						if (step == 10) {
							evaluator.evaluate(9, document, corpus, w);
						}
					}

					return uri;
				}
			});
			w.close();

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Test
	public void testWikipediaCorpus() {

		try {

			initSCOOBIE($DATABASE_SERVER_PC_4327, $DATABASE_DBPEDIA_en2);

			final String template1 = "SELECT * WHERE {?s <http://www.w3.org/2000/01/rdf-schema#label> ?o. }";

			final String template2 = "SELECT * WHERE {?s <http://www.w3.org/2000/01/rdf-schema#label> ?o. ?s1 <"
					+ FOAF_NAME + "> ?o1}";

			final WikipediaCorpus corpus = new WikipediaCorpus();

			final FileWriter w = new FileWriter($SCOOBIE_HOME + "results/"
					+ $DATABASE_DBPEDIA_en2
					+ "/wikipedia_instance_recognition_rating_1.csv");

			corpus.forEach(new DocumentProcedure<URI>() {
				@Override
				public URI process(Reader doc, URI uri) throws Exception {
					Document document = pipeline.createDocument(doc, uri, MediaType.HTML, template1, Language.EN);

					Evaluator evaluator = new Evaluator(pipeline);

					for (int step = 0; pipeline.hasNext(step); step = pipeline
							.execute(step, document)) {

						if (step == 10) {
							evaluator.evaluate(9, document, corpus, w);
						}
					}

					return uri;
				}
			});
			w.close();

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}

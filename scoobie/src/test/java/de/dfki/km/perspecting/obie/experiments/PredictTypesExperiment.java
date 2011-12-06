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
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.ObjectOutputStream;
import java.net.URI;

import opennlp.tools.postag.POSModel;
import opennlp.tools.postag.POSTaggerME;

import org.junit.Test;
import org.postgresql.jdbc2.optional.PoolingDataSource;

import cc.mallet.classify.MaxEnt;
import de.dfki.km.perspecting.obie.connection.KnowledgeBase;
import de.dfki.km.perspecting.obie.connection.PostgresKB;
import de.dfki.km.perspecting.obie.corpus.BBCMusicCorpus;
import de.dfki.km.perspecting.obie.corpus.GutenbergCorpus;
import de.dfki.km.perspecting.obie.corpus.WikinewsCorpus;
import de.dfki.km.perspecting.obie.model.Document;
import de.dfki.km.perspecting.obie.model.Scoobie;
import de.dfki.km.perspecting.obie.model.SemanticEntity;
import de.dfki.km.perspecting.obie.model.TIntDoubleTuple;
import de.dfki.km.perspecting.obie.postprocessor.RDFSerializer;
import de.dfki.km.perspecting.obie.transducer.EntityClassification;
import de.dfki.km.perspecting.obie.transducer.EntityDisambiguation;
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
import de.dfki.km.perspecting.obie.transducer.model.EntityClassifier;
import de.dfki.km.perspecting.obie.transducer.model.MaxentEntityClassifierModel;
import de.dfki.km.perspecting.obie.transducer.model.disambiguation.AmbiguityResolver;
import de.dfki.km.perspecting.obie.transducer.model.disambiguation.DegreeBasedResolver;
import de.dfki.km.perspecting.obie.vocabulary.Language;
import de.dfki.km.perspecting.obie.vocabulary.MediaType;
import de.dfki.km.perspecting.obie.workflow.DummyTask;
import de.dfki.km.perspecting.obie.workflow.Pipeline;

public class PredictTypesExperiment {

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

	public static void setUp(String $DATABASE_SERVER, String $DATABASE,
			String modelPath) throws Exception {

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

		MaxentEntityClassifierModel cl = new MaxentEntityClassifierModel(
				modelPath, Language.EN, new int[] { 1, 2, 3 }, true,
				true, true, 1.0, 4, new String[] { "VB", "ADJ", "NNP", "NN",
						"NNS" });
		cl.load(kb, null);
		EntityClassification namedEntityClassifier = new EntityClassification(
				0.6, cl.getClassifier());
		
		
		pipeline.configure(languageClassification,
				wordTokenizer, sentenceTokenizer, posTagger, nounPhraseChunker,
				suffixArrayBuilder, entityRecognizer, new DummyTask(),
				namedEntityClassifier, subjectResolver, subjectDisambiguator,new DummyTask(),
				new DummyTask(), new DummyTask());
	}

	public static void setUp(String $DATABASE_SERVER, String $DATABASE)
			throws Exception {

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

		pipeline.configure(languageClassification,
				wordTokenizer, sentenceTokenizer, posTagger, nounPhraseChunker,
				suffixArrayBuilder, entityRecognizer, new DummyTask(),
				new DummyTask(), subjectResolver, subjectDisambiguator,new KnownFactsRetrieval(),
				new DummyTask(), new DummyTask());
	}

	final String RDFS_LABEL = "http://www.w3.org/2000/01/rdf-schema#label";
	final String DC_TITLE = "http://purl.org/dc/terms/title";
	final String DC_TITLE_ELEMENTS = "http://purl.org/dc/elements/1.1/title";
	final String WO_SPECIESNAME = "http://purl.org/ontology/wo/speciesName";
	final String FOAF_NAME = "http://xmlns.com/foaf/0.1/name";
	final String OV_SORTLABEL = "http://open.vocab.org/terms/sortLabel";
	final String DBONT_REVIEW = "http://dbpedia.org/ontology/review";

	
	@Test
	public void testWithWikiNews() throws Exception {
		setUp($DATABASE_SERVER_PC_4327, $DATABASE_DBPEDIA_en2, $CORPUS_HOME + "en/wikinews.type.maxent");
		final String template = "SELECT * WHERE {?s <"+RDFS_LABEL+"> ?o}";
		String text = "Silvana is the capital town of England.";

		Document document = pipeline.createDocument(text, new URI("/bla"), MediaType.TEXT, template, Language.EN);

		
		for (int step = 0; pipeline.hasNext(step); step = pipeline
				.execute(step, document)) {
		}
		for(de.dfki.km.perspecting.obie.model.TokenSequence<SemanticEntity> ts : document.getEntityTypes()) {
			System.out.println(ts.toString());
			for(TIntDoubleTuple cl : ts.getValue().getTypeIndex()) {
				System.out.println(kb.getURI(cl.getKey()) + " " + cl.getValue());
			}
		}
		
		BufferedReader r = new BufferedReader(new RDFSerializer("http://this.document").serialize(document, kb));
		for(String line = r.readLine(); line != null; line = r.readLine()) {
			System.out.println(line);
		}
	}
	
	
	@Test
	public void trainWikinewsClassifier() throws Exception {

		setUp($DATABASE_SERVER_PC_4327, $DATABASE_DBPEDIA_en2);

		new WikinewsCorpus().labelRDFTypes(
						new File($CORPUS_HOME + "en/wikinews.type.labeled"),
						pipeline,
						"SELECT * WHERE {?s <" + RDFS_LABEL + "> ?o}")
				.toFeatureFormat(
						new File($CORPUS_HOME
								+ "en/wikinews.type.labeled.feature"),
						new int[] { 1, 2, 3 }, true, true, true, 1.0, 4,
						new String[] { "VB", "ADJ", "NNP", "NN", "NNS" });
		MaxEnt classifier1 = new EntityClassifier(null).train(
				new BufferedReader(new FileReader($CORPUS_HOME
						+ "en/wikinews.type.labeled.feature")), null, null);
		ObjectOutputStream out1 = new ObjectOutputStream(new FileOutputStream(
				$CORPUS_HOME + "en/wikinews.type.maxent"));
		out1.writeObject(classifier1);
		out1.close();

	}

	@Test
	public void trainGutenbergClassifier() throws Exception {

		setUp($DATABASE_SERVER_PC_4327, $DATABASE_DBPEDIA_en2);
		new GutenbergCorpus()
				.labelRDFTypes(
						new File($CORPUS_HOME + "en/gutenberg.type.labeled"),
						pipeline,
						"SELECT * WHERE {?s <" + RDFS_LABEL + "> ?o}")
				.toFeatureFormat(
						new File($CORPUS_HOME
								+ "en/gutenberg.type.labeled.feature"),
						new int[] { 1, 2, 3 }, true, true, true, 1.0, 4,
						new String[] { "VB", "ADJ", "NNP", "NN", "NNS" });
		MaxEnt classifier2 = new EntityClassifier(null).train(
				new BufferedReader(new FileReader($CORPUS_HOME
						+ "en/gutenberg.type.labeled.feature")), null, null);
		ObjectOutputStream out2 = new ObjectOutputStream(new FileOutputStream(
				$CORPUS_HOME + "en/gutenberg.type.maxent"));
		out2.writeObject(classifier2);
		out2.close();

	}

	@Test
	public void trainBBCMusicClassifier() throws Exception {

		setUp($DATABASE_SERVER_PC_4327, $DATABASE_DBPEDIA_en2);
		new BBCMusicCorpus().labelRDFTypes(
				new File($CORPUS_HOME + "en/bbcmusic.type.labeled"),
				pipeline,
				"SELECT * WHERE {?s <" + OV_SORTLABEL + "> ?o . ?s1 <"
						+ FOAF_NAME + "> ?o1 }").toFeatureFormat(
				new File($CORPUS_HOME + "en/bbcmusic.type.labeled.feature"),
				new int[] { 1, 2, 3 }, true, true, true, 1.0, 4,
				new String[] { "VB", "ADJ", "NNP", "NN", "NNS" });
		MaxEnt classifier2 = new EntityClassifier(null).train(
				new BufferedReader(new FileReader($CORPUS_HOME
						+ "en/bbcmusic.type.labeled.feature")), null, null);
		ObjectOutputStream out2 = new ObjectOutputStream(new FileOutputStream(
				$CORPUS_HOME + "en/bbcmusic.type.maxent"));
		out2.writeObject(classifier2);
		out2.close();

	}

}

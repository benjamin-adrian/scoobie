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
import java.util.Map;
import java.util.Map.Entry;

import opennlp.tools.postag.POSModel;
import opennlp.tools.postag.POSTaggerME;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.postgresql.jdbc2.optional.PoolingDataSource;

import cern.colt.Arrays;
import de.dfki.km.perspecting.obie.connection.PostgresKB;
import de.dfki.km.perspecting.obie.corpus.ConllCorpus;
import de.dfki.km.perspecting.obie.corpus.LabeledTextCorpus;
import de.dfki.km.perspecting.obie.corpus.TextCorpus;
import de.dfki.km.perspecting.obie.model.Scoobie;
import de.dfki.km.perspecting.obie.transducer.EntityDisambiguation;
import de.dfki.km.perspecting.obie.transducer.InstanceRecognition;
import de.dfki.km.perspecting.obie.transducer.LanguageIdentification;
import de.dfki.km.perspecting.obie.transducer.POSTagging;
import de.dfki.km.perspecting.obie.transducer.ProperNameRecognition;
import de.dfki.km.perspecting.obie.transducer.RDFLiteralSpotting;
import de.dfki.km.perspecting.obie.transducer.SentenceSegmenter;
import de.dfki.km.perspecting.obie.transducer.SuffixArrayBuilder;
import de.dfki.km.perspecting.obie.transducer.WordSegmenter;
import de.dfki.km.perspecting.obie.transducer.model.CRFNounPhraseChunkerModel;
import de.dfki.km.perspecting.obie.transducer.model.EntityClassifier;
import de.dfki.km.perspecting.obie.transducer.model.LiteralHashing;
import de.dfki.km.perspecting.obie.transducer.model.disambiguation.AmbiguityResolver;
import de.dfki.km.perspecting.obie.transducer.model.disambiguation.DegreeBasedResolver;
import de.dfki.km.perspecting.obie.vocabulary.Language;
import de.dfki.km.perspecting.obie.workflow.DummyTask;
import de.dfki.km.perspecting.obie.workflow.Pipeline;

public class PredictTypesOnConll2003Experiment {

	private static String $PHD_HOME = "/home/adrian/Dokumente/diss/";
	private static String $SCOOBIE_HOME = $PHD_HOME + "scoobie/";
	private static String $CORPUS_HOME = $PHD_HOME + "textcorpus/";

	private static String $DATABASE = "dbpedia_en2";

	private static String $DATABASE_SERVER = "pc-4327.kl.dfki.de";
	private static String $DATABASE_SERVER_USER = "postgres";
	private static String $DATABASE_SERVER_PW = "scoobie";
	private static int $DATABASE_SERVER_PORT = 5432;

	private static PoolingDataSource pool = new PoolingDataSource();

	private static Pipeline pipeline;

	private static PostgresKB kb;

	/**
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {

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
		
		SuffixArrayBuilder suffixArrayBuilder = new SuffixArrayBuilder(100, new LiteralHashing(4));
		RDFLiteralSpotting entityRecognizer = new RDFLiteralSpotting();
		InstanceRecognition subjectResolver = new InstanceRecognition();
		EntityDisambiguation subjectDisambiguator = new EntityDisambiguation(
				new AmbiguityResolver[] { new DegreeBasedResolver() });

		pipeline.configure(languageClassification,
				wordTokenizer, sentenceTokenizer, posTagger, nounPhraseChunker,
				suffixArrayBuilder, entityRecognizer, new DummyTask(),
				new DummyTask(), subjectResolver, subjectDisambiguator,new DummyTask(),
				new DummyTask(), new DummyTask());

	}

	@AfterClass
	public static void shutdownAfterClass() {
		pool.close();
	}

	final int[] ngramsizes = new int[] { 1, 2, 3, 4, 5 };
	final String[] postags = new String[] { "VB", "ADJ", "NNP", "NN", "NNS" };
	final int windowSize = 5;
	final String template = "SELECT * WHERE {"
			+ "?s <http://www.w3.org/2000/01/rdf-schema#label> ?o.}";

	@SuppressWarnings("unchecked")
	@Test
	public void transformConLLDataToFeatureFormat() throws Exception {

		for (String file : new String[] { "eng.testa", "eng.testb", "eng.train" }) {
			ConllCorpus corpus = new ConllCorpus(new File($CORPUS_HOME
					+ "en/conll2003/" + file));

			BufferedReader reader = new BufferedReader(corpus
					.toFeatureFormat(new File($CORPUS_HOME + "en/conll2003/"
							+ file + ".feature"), ngramsizes, true, true, true,
							0, windowSize, postags));
			reader.close();
		}

	}

	@SuppressWarnings("unchecked")
	@Test
	public void transformConLLDataToPlainFormat() throws Exception {

		for (String file : new String[] { "eng.testa", "eng.testb",
				"eng.train", "eng.raw" }) {
			ConllCorpus corpus = new ConllCorpus(new File($CORPUS_HOME
					+ "en/conll2003/" + file));

			corpus.createPlainTextCorpus(new File($CORPUS_HOME
					+ "en/conll2003/plain." + file));

			// labelRDFTypes(corpus, kb, pipeline, file, rootLabels,
			// typeFilter);

		}

	}

	@SuppressWarnings("unchecked")
	@Test
	public void trainClassifierOnConLLData() throws Exception {

		for (String file : new String[] { "eng.testa" }) {
			ConllCorpus corpus = new ConllCorpus(new File($CORPUS_HOME
					+ "en/conll2003/" + file));

			EntityClassifier cl = new EntityClassifier(null);
			BufferedReader reader = new BufferedReader(corpus
					.toFeatureFormat(new File($CORPUS_HOME + "en/conll2003/"
							+ file + ".feature"), ngramsizes, true, true, true,
							1.0, windowSize, postags));

			Map<String, Double[]> quality = cl.evaluate(reader, null, null,
					null, true, 0.8);

			for (Entry<String, Double[]> q : quality.entrySet()) {
				System.out.println(q.getKey() + " "
						+ Arrays.toString(q.getValue()));
			}
		}
	}

//	@SuppressWarnings("unchecked")
//	@Test
//	public void trainClassifierOnDBpediaData() throws Exception {
//		for (String file : new String[] { "plain.eng.testa" }) {
//			TextCorpus corpus = new TextCorpus(new File($CORPUS_HOME
//					+ "en/conll2003/" + file));
//
//			EntityClassifier cl = new EntityClassifier(null);
////
////			LabeledTextCorpus lCorpus = corpus.labelRDFTypes(new File(
////					$CORPUS_HOME + "en/conll2003/" + file + ".labeled"), kb,
////					pipeline, template);
//
//			LabeledTextCorpus lCorpus = new LabeledTextCorpus(new File(
//					$CORPUS_HOME + "en/conll2003/" + file + ".labeled"));
//
//			BufferedReader reader = new BufferedReader(lCorpus
//					.toFeatureFormat(new File($CORPUS_HOME + "en/conll2003/"
//							+ file + ".feature"), ngramsizes, true, true, true,
//							1.0, windowSize, postags));
//
//			Map<String, Double[]> quality = cl.evaluate(reader, null, null,
//					null, true, 0.8);
//
//			for (Entry<String, Double[]> q : quality.entrySet()) {
//				System.out.println(kb.getURI(Integer.parseInt(q.getKey())) + "(" + q.getKey()+ ")"
//						+ Arrays.toString(q.getValue()));
//			}
//		}
//	}

}

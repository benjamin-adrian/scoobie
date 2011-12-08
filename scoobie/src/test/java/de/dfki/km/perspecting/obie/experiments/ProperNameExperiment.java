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

/**
 * 
 */
import java.net.URI;
import java.net.URL;
import java.util.Arrays;
import java.util.HashSet;

import opennlp.tools.postag.POSModel;
import opennlp.tools.postag.POSTaggerME;

import org.apache.commons.io.FileUtils;
import org.junit.Test;
import org.postgresql.jdbc2.optional.PoolingDataSource;

import de.dfki.km.perspecting.obie.connection.KnowledgeBase;
import de.dfki.km.perspecting.obie.connection.PostgresKB;
import de.dfki.km.perspecting.obie.corpus.BBCMusicCorpus;
import de.dfki.km.perspecting.obie.corpus.BBCNatureCorpus;
import de.dfki.km.perspecting.obie.corpus.WikipediaCorpus;
import de.dfki.km.perspecting.obie.model.Document;
import de.dfki.km.perspecting.obie.model.Scoobie;
import de.dfki.km.perspecting.obie.model.Token;
import de.dfki.km.perspecting.obie.model.TokenSequence;
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

/**
 * @author adrian
 * @version 0.1
 * @since 09.10.2009
 * 
 */
public class ProperNameExperiment {

	/**
	 * 
	 */

	private static PoolingDataSource pool = new PoolingDataSource();

	private static Pipeline pipeline;

	/******************* technical setup ******************************************/

	private static String $PHD_HOME = "/home/adrian/Dokumente/diss/";
	private static String $SCOOBIE_HOME = $PHD_HOME + "scoobie/";
	private static String $CORPUS_HOME = $PHD_HOME + "textcorpus/";

	private static String $DATABASE = "dbpedia_en2";

	private static String $DATABASE_SERVER_USER = "postgres";
	private static String $DATABASE_SERVER_PW = "scoobie";
	private static int $DATABASE_SERVER_PORT = 5432;

	private static String $DATABASE_DBPEDIA_en2 = "dbpedia_en2";
	private static String $DATABASE_BBC_MUSIC = "bbc_music";
	private static String $DATABASE_BBC_WILDLIFE = "bbc_wildlife";
	
	private static String $DATABASE_SERVER_PC_4327 = "pc-4327.kl.dfki.de";
	private static String $DATABASE_SERVER_LOCALHOST = "localhost";
	
	private static KnowledgeBase kb;

	/**
	 * @throws java.lang.Exception
	 */

	public static void setUp(String databaseServer, String dataBase) throws Exception {

		pool.setUser($DATABASE_SERVER_USER);
		pool.setPassword($DATABASE_SERVER_PW);
		pool.setPortNumber($DATABASE_SERVER_PORT);
		pool.setDatabaseName(dataBase);
		pool.setServerName(databaseServer);
		pool.setMaxConnections(100);

		kb = new PostgresKB(pool.getConnection(), dataBase, new URI("http://test.de"));
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
		pipeline.configure(languageClassification,
				wordTokenizer, sentenceTokenizer, posTagger, nounPhraseChunker,
				suffixArrayBuilder, entityRecognizer, new DummyTask(),
				new DummyTask(), new DummyTask(), new DummyTask(),new DummyTask(),
				new DummyTask(), new DummyTask());

	}

	@Test
	public void testOnDBpedia() throws Exception {
		setUp($DATABASE_SERVER_PC_4327, $DATABASE_DBPEDIA_en2);
		
		kb.calculateProperNameStatistics(new WikipediaCorpus(), pipeline);
		
		pool.close();
	}
	
	@Test
	public void testOnBBCNature() throws Exception {
		setUp($DATABASE_SERVER_LOCALHOST, $DATABASE_BBC_WILDLIFE);
		
		kb.calculateProperNameStatistics(new BBCNatureCorpus(), pipeline);
		
		pool.close();
	}
	
	@Test
	public void testOnBBCMusic() throws Exception {
		setUp($DATABASE_SERVER_LOCALHOST, $DATABASE_BBC_MUSIC);
		
		kb.calculateProperNameStatistics(new BBCMusicCorpus(), pipeline);
		
		pool.close();
	}

	/**
	 * Test method for
	 * {@link de.dfki.km.perspecting.obie.dixi.service.SimpleScobieService#extractInformationFromURL(java.lang.String, java.lang.String)}
	 * .
	 */
	@Test
	public void testExtractInformationFromURL() {
		try {
			StringBuffer b = new StringBuffer();

			for (int i = 0; i < 1; i++) {

				
				Document document = pipeline
						.createDocument(
								FileUtils
										.toFile(new URL(
												"http://en.wikipedia.org/wiki/Special:Random")),
								new URI(
										"http://en.wikipedia.org/wiki/Special:Random"),
								MediaType.HTML, "SELECT * WHERE {?s ?p ?o}", Language.EN);

				
				
				Evaluator evaluator = new Evaluator(pipeline);

				for (int step = 0; pipeline.hasNext(step) && step <= 5; step = pipeline
						.execute(step, document)) {
					System.out.println(step);
				}

				HashSet<String> wordsOfPhrases = new HashSet<String>();
				HashSet<String> wordsOfDocument = new HashSet<String>();

				for (Token token : document.getTokens()) {
					wordsOfDocument.add(token.toString());
				}

				int count = 0;
				for (TokenSequence<String> np : document.getNounPhrases()) {
					String[] words = np.toString().split("[\\s]+");
					count += words.length;
					wordsOfPhrases.addAll(Arrays.asList(words));
				}

				b.append(document.getTokens().size() + "\t"
						+ document.getNounPhrases().size() + "\t" + count
						+ "\t" + wordsOfPhrases.size() + "\t"
						+ wordsOfDocument.size() + "\n");

			}
			System.out
					.println("tok in doc\tnp in doc\ttok in nps\tdistinct tok in nps\tdistinct tok in doc");
			System.out.println(b);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}

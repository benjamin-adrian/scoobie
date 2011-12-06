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
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.Reader;
import java.net.URI;
import java.net.URL;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.HashSet;
import java.util.TreeSet;

import opennlp.tools.postag.POSModel;
import opennlp.tools.postag.POSTaggerME;

import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.postgresql.jdbc2.optional.PoolingDataSource;

import de.dfki.km.perspecting.obie.connection.PostgresKB;
import de.dfki.km.perspecting.obie.corpus.TextCorpus;
import de.dfki.km.perspecting.obie.model.Document;
import de.dfki.km.perspecting.obie.model.DocumentProcedure;
import de.dfki.km.perspecting.obie.model.Scoobie;
import de.dfki.km.perspecting.obie.model.Token;
import de.dfki.km.perspecting.obie.model.TokenSequence;
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
import de.dfki.km.perspecting.obie.workflow.Pipeline;

/**
 * @author adrian
 * @version 0.1
 * @since 09.10.2009
 * 
 */
public class PhraseExperiment {

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

		kb = new PostgresKB(pool.getConnection(), $DATABASE, new URI(
				"http://test.de"));
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

		pipeline.configure(languageClassification, wordTokenizer,
				sentenceTokenizer, posTagger, nounPhraseChunker,
				suffixArrayBuilder, entityRecognizer, new DummyTask(),
				new DummyTask(), subjectResolver, new DummyTask(),
				new DummyTask(), new DummyTask(), new DummyTask());

	}

	@AfterClass
	public static void shutdownAfterClass() {
		pool.close();
	}

	@Test
	public void analysePhraseLength() throws Exception {
		final BufferedWriter bw = new BufferedWriter(new FileWriter(
				$SCOOBIE_HOME + "results/token_length_histogram.csv"));
		Connection conn = pool.getConnection();
		ResultSet rs = conn
				.createStatement()
				.executeQuery(
						"SELECT length(literal), count(*)  FROM index_literals GROUP BY length(literal) ORDER BY length(literal)");
		while (rs.next()) {
			bw.append(rs.getInt(1) + "\t" + rs.getInt(2));
			bw.newLine();
		}
		bw.close();
	}

	/**
	 * Test method for
	 * {@link de.dfki.km.perspecting.obie.dixi.service.SimpleScobieService#extractInformationFromURL(java.lang.String, java.lang.String)}
	 * .
	 */
	@Test
	public void analyseTokenPhraseFrequencies() {
		final String template = "SELECT * WHERE {?s ?p ?o}";

		try {
			final BufferedWriter bw = new BufferedWriter(new FileWriter(
					$SCOOBIE_HOME
							+ "results/token_phrase_frequency_wikipedia.csv"));

			final String randomWikipediaPage = "http://en.wikipedia.org/wiki/Special:Random";

			bw.append("tok in doc\tnp in doc\ttok in nps\tdistinct tok in nps\tdistinct tok in doc");
			for (int i = 0; i < 100; i++) {

				Document document = pipeline
						.createDocument(
								FileUtils
										.toFile(new URL(
												randomWikipediaPage)),
								new URI(
										randomWikipediaPage),
								MediaType.HTML, template, Language.EN);

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

				bw.append(document.getTokens().size() + "\t"
						+ document.getNounPhrases().size() + "\t" + count
						+ "\t" + wordsOfPhrases.size() + "\t"
						+ wordsOfDocument.size());
				bw.newLine();

			}
			bw.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		try {
			final BufferedWriter bw = new BufferedWriter(new FileWriter(
					$SCOOBIE_HOME
							+ "results/token_phrase_frequency_reuters.csv"));

			final TextCorpus corpus = new TextCorpus(new File("../corpora/reuters/reuters.zip"), MediaType.ZIP, MediaType.HTML, Language.EN);

			bw.append("tok in doc\tnp in doc\ttok in nps\tdistinct tok in nps\tdistinct tok in doc");

			corpus.forEach(new DocumentProcedure<URI>() {

				@Override
				public URI process(Reader reader, URI uri)
						throws Exception {

					Document document = pipeline.createDocument(reader, uri, corpus.getMediatype(), template, corpus.getLanguage());

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

					bw.append(document.getTokens().size() + "\t"
							+ document.getNounPhrases().size() + "\t" + count
							+ "\t" + wordsOfPhrases.size() + "\t"
							+ wordsOfDocument.size());
					bw.newLine();
					return uri;
				}
			});

			bw.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@Test
	public void testDifferentPrefixLengths() {
		
		
		final String template = "SELECT * WHERE {?s ?p ?o}";
		try {
			URL url = new URL("http://en.wikipedia.org/wiki/Kaiserslautern");
			Document document = pipeline.createDocument(FileUtils.toFile(url), url.toURI(), MediaType.HTML, template, Language.EN);
			for (int step = 0; pipeline.hasNext(step) && step <= 5; step = pipeline
					.execute(step, document)) {
				System.out.println(step);
			}
			final BufferedWriter bw = new BufferedWriter(new FileWriter(
					$SCOOBIE_HOME + "results/response_time_prefix_hashing.csv"));

			for (int SIZE = 1; SIZE < 11; SIZE++) {

				TreeSet<String> hist = new TreeSet<String>();

				int count = 0;

				for (TokenSequence<String> i : document.getNounPhrases()) {
					String[] words = i.toString().split("[\\s]+");
					for (String word : words) {
						count++;
						if (word.length() >= SIZE)
							hist.add(word.substring(0, SIZE));
						else
							hist.add(word);
					}
				}

				StringBuilder query = new StringBuilder();

				query.append("SELECT count(*) FROM index_literals, symbols WHERE "
						+ "( symbols.object = index_literals.index AND substr(index_literals.literal,1,"
						+ SIZE + ") IN (");

				for (String p : hist) {
					query.append("(?) , ");
				}

				query.setLength(query.length() - 3);
				query.append("))");
				System.out.println(query.toString());

				Connection c = pool.getConnection();
				PreparedStatement stmtGetDatatypePropertyValues = c
						.prepareStatement(query.toString());
				int paramIndex = 0;
				for (String p : hist) {
					stmtGetDatatypePropertyValues.setString(++paramIndex, p);
				}
				long start = System.currentTimeMillis();
				ResultSet rs = stmtGetDatatypePropertyValues.executeQuery();
				long end = System.currentTimeMillis();
				while (rs.next()) {
					bw.append(SIZE + "\t" + (end - start) + "\t" + rs.getInt(1));
					bw.newLine();
				}
				stmtGetDatatypePropertyValues.close();
				c.close();

			}
			bw.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}

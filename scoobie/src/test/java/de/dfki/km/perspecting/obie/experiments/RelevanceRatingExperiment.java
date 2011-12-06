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
import gnu.trove.TDoubleFunction;
import gnu.trove.TIntHashSet;
import gnu.trove.TObjectDoubleHashMap;
import gnu.trove.TObjectDoubleProcedure;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileFilter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Locale;

import opennlp.tools.postag.POSModel;
import opennlp.tools.postag.POSTaggerME;

import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.Test;
import org.postgresql.jdbc2.optional.PoolingDataSource;

import cern.colt.function.DoubleFunction;
import cern.colt.matrix.DoubleMatrix2D;
import cern.colt.matrix.doublealgo.Formatter;
import cern.colt.matrix.impl.DenseDoubleMatrix2D;
import de.dfki.km.perspecting.obie.connection.KnowledgeBase;
import de.dfki.km.perspecting.obie.connection.PostgresKB;
import de.dfki.km.perspecting.obie.corpus.BBCMusicCorpus;
import de.dfki.km.perspecting.obie.corpus.BBCNatureCorpus;
import de.dfki.km.perspecting.obie.corpus.GutenbergCorpus;
import de.dfki.km.perspecting.obie.corpus.TextCorpus;
import de.dfki.km.perspecting.obie.corpus.WikinewsCorpus;
import de.dfki.km.perspecting.obie.corpus.WikipediaCorpus;
import de.dfki.km.perspecting.obie.model.Document;
import de.dfki.km.perspecting.obie.model.DocumentProcedure;
import de.dfki.km.perspecting.obie.model.DoubleMatrix;
import de.dfki.km.perspecting.obie.model.Scoobie;
import de.dfki.km.perspecting.obie.transducer.EntityDisambiguation;
import de.dfki.km.perspecting.obie.transducer.InstanceRecognition;
import de.dfki.km.perspecting.obie.transducer.KnownFactsRetrieval;
import de.dfki.km.perspecting.obie.transducer.LanguageIdentification;
import de.dfki.km.perspecting.obie.transducer.POSTagging;
import de.dfki.km.perspecting.obie.transducer.ProperNameRecognition;
import de.dfki.km.perspecting.obie.transducer.RDFLiteralSpotting;
import de.dfki.km.perspecting.obie.transducer.RelevanceRating;
import de.dfki.km.perspecting.obie.transducer.SentenceSegmenter;
import de.dfki.km.perspecting.obie.transducer.SuffixArrayBuilder;
import de.dfki.km.perspecting.obie.transducer.WordSegmenter;
import de.dfki.km.perspecting.obie.transducer.model.CRFNounPhraseChunkerModel;
import de.dfki.km.perspecting.obie.transducer.model.disambiguation.AmbiguityResolver;
import de.dfki.km.perspecting.obie.transducer.model.disambiguation.DegreeBasedResolver;
import de.dfki.km.perspecting.obie.transducer.model.rating.AuthorityBasedRating;
import de.dfki.km.perspecting.obie.transducer.model.rating.CapacityBasedRating;
import de.dfki.km.perspecting.obie.transducer.model.rating.DegreeBasedRating;
import de.dfki.km.perspecting.obie.transducer.model.rating.HubBasedRating;
import de.dfki.km.perspecting.obie.transducer.model.rating.InverseDocumentFrequencyBasedRating;
import de.dfki.km.perspecting.obie.transducer.model.rating.PageRankBasedRating;
import de.dfki.km.perspecting.obie.transducer.model.rating.PositionBasedRating;
import de.dfki.km.perspecting.obie.transducer.model.rating.RandomRating;
import de.dfki.km.perspecting.obie.transducer.model.rating.RatingMetric;
import de.dfki.km.perspecting.obie.transducer.model.rating.TermFrequencyBasedRating;
import de.dfki.km.perspecting.obie.vocabulary.Language;
import de.dfki.km.perspecting.obie.vocabulary.MediaType;
import de.dfki.km.perspecting.obie.workflow.DummyTask;
import de.dfki.km.perspecting.obie.workflow.Evaluator;
import de.dfki.km.perspecting.obie.workflow.Pipeline;

public class RelevanceRatingExperiment {

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
			TextCorpus corpus) throws Exception {

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


		POSModel posModel = new POSModel(Scoobie.class.getResourceAsStream("pos/en/en-pos-maxent.bin"));
		POSTagging posTagger = new POSTagging(new POSTaggerME(posModel));

		

		ProperNameRecognition nounPhraseChunker = new ProperNameRecognition(
				new CRFNounPhraseChunkerModel($SCOOBIE_HOME
						+ $DATABASE_DBPEDIA_en2 + "/npc/en/EN.crf"));

		SuffixArrayBuilder suffixArrayBuilder = new SuffixArrayBuilder(100);
		RDFLiteralSpotting namedEntityRecognizer = new RDFLiteralSpotting();
		InstanceRecognition instanceResolver = new InstanceRecognition();
		EntityDisambiguation instanceDisambiguator = new EntityDisambiguation(
				new AmbiguityResolver[] { new DegreeBasedResolver() });

		KnownFactsRetrieval factRetrieval = new KnownFactsRetrieval();

		ArrayList<int[]> l = new ArrayList<int[]>();

		int max = (int) Math.pow(2, 9);
		for (int i = 0; i < max; i++) {
			String binary = Integer.toBinaryString(i);
			String prefix = "";
			for (int pad = 0; pad < 9 - binary.length(); pad++) {
				prefix += "0";
			}
			binary = prefix + binary;

			TIntHashSet s = new TIntHashSet();
			for (int j = 0; j < 9; j++) {
				if (j < binary.length() && binary.charAt(j) == '1') {
					s.add(j);
				}
			}
			if (s.size() > 1)
				l.add(s.toArray());
		}

		RelevanceRating relevanceRating = new RelevanceRating(
				new RatingMetric[] {
						new AuthorityBasedRating(), // 0
						new HubBasedRating(), // 1
						new PageRankBasedRating(), // 2
						new DegreeBasedRating(), // 3
						new CapacityBasedRating(), // 4
						new RandomRating(), // 5
						new PositionBasedRating(), // 6
						new TermFrequencyBasedRating(), // 7
						new InverseDocumentFrequencyBasedRating(corpus,
								new File(corpus.getCorpus().getAbsolutePath()
										+ "/index/")) }, // 8

				l.toArray(new int[l.size()][]));

		pipeline.configure(languageClassification, wordTokenizer,
				sentenceTokenizer, posTagger, nounPhraseChunker,
				suffixArrayBuilder, namedEntityRecognizer, new DummyTask(),
				new DummyTask(), instanceResolver, instanceDisambiguator,
				factRetrieval, relevanceRating, new DummyTask());

	}

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
			final WikinewsCorpus corpus = new WikinewsCorpus();

			setUp($DATABASE_SERVER_PC_4327, $DATABASE_DBPEDIA_en2, corpus);

			final FileWriter w = new FileWriter($SCOOBIE_HOME + "results/"
					+ $DATABASE_DBPEDIA_en2
					+ "/relevance_wikinews_combined.csv");

			corpus.forEach(new DocumentProcedure<String>() {
				@Override
				public String process(Reader doc, URI uri) throws Exception {
					String template = "SELECT * FROM NAMED <http://port-41xy?graph="
							+ kb.getUri()
							+ "&doc="
							+ uri.toString()
							+ "#recognized> WHERE {GRAPH <http://port-41xy?graph="
							+ kb.getUri()
							+ "&doc="
							+ uri.toString()
							+ "#recognized> {?s <" + RDFS_LABEL + "> ?o}}";

					Document document = pipeline.createDocument(new File(uri),
							uri, corpus.getMediatype(), template, corpus
									.getLanguage());
					Evaluator evaluator = new Evaluator(pipeline);

					for (int step = 0; pipeline.hasNext(step); step = pipeline
							.execute(step, document)) {

						if (step == 13) {
							evaluator.evaluate(12, document, corpus, w);

							printMatrixDumps(document.getRelevanceScores(),
									document.getUri().toURL().getFile(),
									$SCOOBIE_HOME + "results/"
											+ $DATABASE_DBPEDIA_en2
											+ "/correlation1/wikinews/");
						}

					}

					return uri.toString();
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
			final GutenbergCorpus corpus = new GutenbergCorpus();

			setUp($DATABASE_SERVER_PC_4327, $DATABASE_DBPEDIA_en2, corpus);

			final FileWriter w = new FileWriter($SCOOBIE_HOME + "results/"
					+ $DATABASE_DBPEDIA_en2
					+ "/relevance_gutenberg_combined.csv");

			corpus.forEach(new DocumentProcedure<String>() {
				@Override
				public String process(Reader doc, URI uri) throws Exception {

					String template = "SELECT * FROM NAMED <http://port-41xy?graph="
							+ kb.getUri()
							+ "&doc="
							+ uri.toString()
							+ "#recognized> "
							+ "WHERE {GRAPH <http://port-41xy?graph="
							+ kb.getUri()
							+ "&doc="
							+ uri.toString()
							+ "#recognized> " + "{?s <" + RDFS_LABEL + "> ?o}}";

					Document document = pipeline.createDocument(new File(uri),
							uri, corpus.getMediatype(), template, corpus
									.getLanguage());
					Evaluator evaluator = new Evaluator(pipeline);

					for (int step = 0; pipeline.hasNext(step); step = pipeline
							.execute(step, document)) {

						if (step == 13) {
							evaluator.evaluate(12, document, corpus, w);

							printMatrixDumps(document.getRelevanceScores(),
									document.getUri().toURL().getFile(),
									$SCOOBIE_HOME + "results/"
											+ $DATABASE_DBPEDIA_en2
											+ "/correlation/gutenberg/");
						}

					}

					return uri.toString();
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
	public void testBBCMusicCorpus() {
		try {

			final BBCMusicCorpus corpus = new BBCMusicCorpus();

			setUp($DATABASE_SERVER_LOCALHOST, $DATABASE_BBC_MUSIC, corpus);

			final FileWriter w = new FileWriter($SCOOBIE_HOME + "results/"
					+ $DATABASE_BBC_MUSIC + "/relevance_bbc_music_combined.csv");

			corpus.forEach(new DocumentProcedure<String>() {
				@Override
				public String process(Reader doc, URI uri) throws Exception {

					String template = "SELECT * FROM NAMED <http://port-41xy?graph="
							+ kb.getUri()
							+ "&doc="
							+ uri.toString()
							+ "#recognized> "
							+ "WHERE {GRAPH <http://port-41xy?graph="
							+ kb.getUri()
							+ "&doc="
							+ uri.toString()
							+ "#recognized> "
							+ "{?s <"
							+ OV_SORTLABEL
							+ "> ?o . ?s1 <" + FOAF_NAME + "> ?o1 }" + "}";

					Document document = pipeline.createDocument(new File(uri),
							uri, corpus.getMediatype(), template, corpus
									.getLanguage());
					Evaluator evaluator = new Evaluator(pipeline);

					for (int step = 0; pipeline.hasNext(step); step = pipeline
							.execute(step, document)) {

						if (step == 13) {
							printMatrixDumps(document.getRelevanceScores(),
									document.getUri().toURL().getFile(),
									$SCOOBIE_HOME + "results/"
											+ $DATABASE_BBC_MUSIC
											+ "/correlation/");
							evaluator.evaluate(12, document, corpus, w);
						}

					}

					return uri.toString();
				}
			});
			w.close();

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * @param matrix
	 * @param doc
	 * @throws IOException
	 */
	private void printMatrixDumps(DoubleMatrix matrix, String name, String path)
			throws IOException {
		Formatter f = new Formatter();
		f.setFormat("%1.1f");
		f.setColumnSeparator("\t");
		FileUtils.writeStringToFile(
				new File(path + "/" + name + ".pearson.csv"), f.toString(matrix
						.pearsonCorrelationDoubleMatrix()), "utf-8");
		FileUtils.writeStringToFile(new File(path + "/" + name
				+ ".spearman.csv"), f.toString(matrix
				.spearmanCorrelationMatrix()), "utf-8");

	}

	@Test
	public void testBBCNatureCorpus() {
		try {
			final BBCNatureCorpus corpus = new BBCNatureCorpus();

			setUp($DATABASE_SERVER_LOCALHOST, $DATABASE_BBC_WILDLIFE, corpus);

			final FileWriter w = new FileWriter($SCOOBIE_HOME + "results/"
					+ $DATABASE_BBC_WILDLIFE
					+ "/relevance_bbc_nature_combined.csv");

			corpus.forEach(new DocumentProcedure<String>() {
				@Override
				public String process(Reader doc, URI uri) throws Exception {

					String template = "SELECT * FROM NAMED <http://port-41xy?graph="
							+ kb.getUri()
							+ "&doc="
							+ uri.toString()
							+ "#recognized> "
							+ "WHERE {GRAPH <http://port-41xy?graph="
							+ kb.getUri()
							+ "&doc="
							+ uri.toString()
							+ "#recognized> "
							+ "{?s <"
							+ RDFS_LABEL
							+ "> ?o . ?s1 <" + DC_TITLE + "> ?o1 }" + "}";

					Document document = pipeline.createDocument(new File(uri),
							uri, corpus.getMediatype(), template, corpus
									.getLanguage());
					Evaluator evaluator = new Evaluator(pipeline);

					for (int step = 0; pipeline.hasNext(step); step = pipeline
							.execute(step, document)) {

						if (step == 13) {
							printMatrixDumps(document.getRelevanceScores(),
									document.getUri().toURL().getFile(),
									$SCOOBIE_HOME + "results/"
											+ $DATABASE_BBC_WILDLIFE
											+ "/correlation/");
							evaluator.evaluate(12, document, corpus, w);
						}

					}

					return uri.toString();
				}
			});
			w.close();

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Test
	public void avgCorrelations() throws Exception {

		String[] corpora = new String[] { "wikinews", "wikipedia", "gutenberg",
				"bbc_music", "bbc_nature" };

		for (String corpus : corpora) {

			final String path = "/home/adrian/Dokumente/diss/scoobie/results/dbpedia_en2/correlation/"
					+ corpus + "/";
			// final String type = "pearson.csv";
			final String[] types = new String[] { "pearson.csv", "spearman.csv" };

			for (final String type : types) {

				File folder = new File(path);
				final File[] files = folder.listFiles(new FileFilter() {

					@Override
					public boolean accept(File pathname) {
						return pathname.getName().endsWith(type);
					}
				});

				DoubleMatrix2D m = new DenseDoubleMatrix2D(9, 9);
				m.assign(new DoubleFunction() {
					@Override
					public double apply(double arg0) {
						return 0;
					}
				});

				for (File file : files) {
					BufferedReader br = new BufferedReader(new FileReader(file));
					br.readLine();
					int row = 0;

					try {
						for (String line = br.readLine(); line != null; line = br
								.readLine()) {
							String[] items = line.split("\t");

							for (int col = 0; col < 9; col++) {
								double d = Double.parseDouble(items[col]);
								if (Double.isNaN(d))
									d = 0.0;
								if (d < 0.001)
									d = 0.0;
								if (d > 1)
									d = 1;
								m.set(row, col, m.get(row, col) + d);
							}

							row++;
							if (row == 9)
								break;
						}
					} catch (Exception e) {
						throw new Exception(file.getName(), e);
					}
					br.close();
					// System.out.println(m);
				}

				final double count = m.get(0, 0);

				m.assign(new DoubleFunction() {
					@Override
					public double apply(double arg0) {
						return arg0 / count;
					}
				});

				BufferedWriter w = new BufferedWriter(new FileWriter(
						"/home/adrian/Dokumente/diss/scoobie/results/heatmap."
								+ corpus + "." + type + ".gnup"));
				w.append("set terminal svg size 600,600 dynamic enhanced fname 'times'  fsize 12 butt solid\n");
				w.append("set output 'heatmaps."+corpus + "." + type + ".svg'\n");
				w.append("unset key\n");
				w.append("set view map\n");
				w.append("set style data linespoints\n");
				w.append("set xtics border in scale 0,0 mirror norotate  offset character 0, 0, 0\n");
				w.append("set ytics border in scale 0,0 mirror norotate  offset character 0, 0, 0\n");
				w.append("set xrange [ -0.500000 : 8.50000 ] noreverse nowriteback\n");
				w.append("set yrange [ -0.500000 : 8.50000 ] reverse nowriteback\n");
				w.append("set palette rgbformulae 2, -7, -7\n");
				w.append("splot '-' matrix with image\n");
				Formatter f = new Formatter();
				f.setFormat("%1.1f");
				f.setColumnSeparator(" ");
				w.append("#");
				w.append(f.toString(m));
				w.close();
				
				// FileUtils.writeStringToFile(new
				// File("/home/adrian/Dokumente/diss/scoobie/results/bbc_wildlife/correlation/"
				// + doc[doc.length - 1] + ".pearson.csv"),
				// f.toString(matrix.pearsonCorrelationDoubleMatrix()),
				// "utf-8");
			}
		}
	}

	@Test
	public void testWikipediaCorpus() {
		try {
			final File wikinewsFolder = new File($CORPUS_HOME
					+ "en/wikipedia2010/text");
			final WikipediaCorpus corpus = new WikipediaCorpus();
			setUp($DATABASE_SERVER_PC_4327, $DATABASE_DBPEDIA_en2, corpus);

			final FileWriter w = new FileWriter($SCOOBIE_HOME + "results/"
					+ $DATABASE_DBPEDIA_en2
					+ "/relevance_wikipedia_combined.csv");

			corpus.forEach(new DocumentProcedure<String>() {
				@Override
				public String process(Reader doc, URI uri) throws Exception {

					String template = "SELECT * FROM NAMED <http://port-41xy?graph="
							+ kb.getUri()
							+ "&doc="
							+ uri.toString()
							+ "#recognized> "
							+ "WHERE {GRAPH <http://port-41xy?graph="
							+ kb.getUri()
							+ "&doc="
							+ uri.toString()
							+ "#recognized> " + "{?s <" + RDFS_LABEL + "> ?o}}";

					Document document = pipeline.createDocument(new File(uri),
							uri, corpus.getMediatype(), template, corpus
									.getLanguage());
					Evaluator evaluator = new Evaluator(pipeline);

					for (int step = 0; pipeline.hasNext(step); step = pipeline
							.execute(step, document)) {

						if (step == 13) {
							evaluator.evaluate(12, document, corpus, w);

							printMatrixDumps(document.getRelevanceScores(),
									document.getUri().toURL().getFile(),
									$SCOOBIE_HOME + "results/"
											+ $DATABASE_DBPEDIA_en2
											+ "/correlation/wikipedia/");
						}

					}

					return uri.toString();
				}
			});
			w.close();

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@Test
	public void analyseMeanRatings() throws Exception {

		String[] paths = new String[] {
		// "/home/adrian/Dokumente/diss/scoobie/results/dbpedia_en2/relevance_wikipedia_combined.csv"};
		// "/home/adrian/Dokumente/diss/scoobie/results/dbpedia_en2/relevance_gutenberg_combined.csv"};
		"/home/adrian/Dokumente/diss/scoobie/results/dbpedia_en2/relevance_wikinews_combined.csv" };
		// "/home/adrian/Dokumente/diss/scoobie/results/bbc_wildlife/relevance_bbc_nature_combined.csv"};
		// "/home/adrian/Dokumente/diss/scoobie/results/bbc_music/relevance_bbc_music_combined.csv"
		// };
		for (String path : paths) {
			System.out.println(path);
			BufferedReader r = new BufferedReader(new FileReader(path));

			TObjectDoubleHashMap<String> map = new TObjectDoubleHashMap<String>();
			final HashSet<String> docs = new HashSet<String>();

			int i = 0;
			for (String line = r.readLine(); line != null; line = r.readLine()) {
				String[] row = line.split("\t");
				double rel = Double.parseDouble(row[2].replace(",", "."));
				map.adjustOrPutValue(row[1], rel, rel);
				docs.add(row[0]);

				// if (i < 9) {
				// System.out.println((i++) + ": " + row[1]);
				// }
			}

			map.transformValues(new TDoubleFunction() {

				@Override
				public double execute(double value) {
					return value / docs.size();
				}
			});

			map.forEachEntry(new TObjectDoubleProcedure<String>() {

				@Override
				public boolean execute(String a, double b) {
					a = a.replaceAll("\\[", "").replaceAll("\\]", "");
					a = a.replace("0", "AuthorityBasedRating");
					a = a.replace("1", "HubBasedRating");
					a = a.replace("2", "PageRankBasedRating");
					a = a.replace("3", "DegreeBasedRating");
					a = a.replace("4", "CapacityBasedRating");
					a = a.replace("5", "RandomRating");
					a = a.replace("6", "PositionBasedRating");
					a = a.replace("7", "TermFrequencyBasedRating");
					a = a.replace("8", "InverseDocumentFrequencyBasedRating");
					System.out.println(String.format(Locale.US, "%s\t%1.3f", a,
							b));
					return true;
				}
			});

			System.out
					.println("-------------------------------------------------------");
		}

	}

}

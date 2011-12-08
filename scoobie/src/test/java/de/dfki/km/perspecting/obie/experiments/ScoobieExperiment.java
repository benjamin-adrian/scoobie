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
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import de.dfki.km.perspecting.obie.corpus.TextCorpus;
import de.dfki.km.perspecting.obie.corpus.WikinewsCorpus;
import de.dfki.km.perspecting.obie.model.Document;
import de.dfki.km.perspecting.obie.model.DocumentProcedure;
import de.dfki.km.perspecting.obie.model.Scoobie;
import de.dfki.km.perspecting.obie.postprocessor.ListSerializer;
import de.dfki.km.perspecting.obie.vocabulary.Language;
import de.dfki.km.perspecting.obie.vocabulary.MediaType;

public class ScoobieExperiment {

	/******************* technical setup ******************************************/

	private static String $PHD_HOME = "/home/adrian/Dokumente/diss/";
	private static String $CORPUS_HOME = $PHD_HOME + "textcorpus/";

	private static String $DATABASE_DBPEDIA_en2 = "dbpedia_en2";

	private static String $DATABASE_SERVER_PC_4327 = "pc-4327.kl.dfki.de";
	private static String $DATABASE_SERVER_LOCALHOST = "localhost";

	private static String $DATABASE_SERVER_USER = "postgres";
	private static String $DATABASE_SERVER_PW = "scoobie";
	private static int $DATABASE_SERVER_PORT = 5432;
	
	
	/******************* vocabulary ******************************************/
	
	private final String RDFS_LABEL = "http://www.w3.org/2000/01/rdf-schema#label";

	
	
	@SuppressWarnings("unchecked")
	@Test
	public void testSystem() throws Exception {

		final WikinewsCorpus corpus = new WikinewsCorpus(
				new File(
						"../corpora/wikinews/wikinews_text_labels.zip"),
				new TextCorpus(
						new File(
								"../corpora/wikinews/wikinews_text_labels.zip"),
						MediaType.ZIP, MediaType.HTML, Language.EN));
		

		final Scoobie $ = new Scoobie($DATABASE_DBPEDIA_en2,
				$DATABASE_SERVER_USER, $DATABASE_SERVER_PW,
				$DATABASE_SERVER_PORT, $DATABASE_SERVER_PC_4327, $CORPUS_HOME
						+ "en/wikipedia.type.maxent", corpus, new URI(
						"http://dbpedia.org"));
		final ListSerializer s = new ListSerializer(0);

		final int k = 1000;

		List<float[]> results = (List<float[]>) corpus
				.forEach(new DocumentProcedure<float[]>() {
					@Override
					public float[] process(Reader doc, URI uri)
							throws Exception {
						String template = "SELECT * FROM NAMED <http://port-41xy?graph="
								+ $.kb().getUri()
								+ "&doc="
								+ uri.toString()
								+ "#recognized> WHERE {GRAPH <http://port-41xy?graph="
								+ $.kb().getUri()
								+ "&doc="
								+ uri.toString()
								+ "#recognized> {?s <" + RDFS_LABEL + "> ?o}}";
						Document document = $.pipeline().createDocument(doc, uri,
								corpus.getMediatype(), template, corpus
										.getLanguage());
						for (int step = 0; $.pipeline().hasNext(step); step = $.pipeline()
								.execute(step, document))
							;
						BufferedReader recos = new BufferedReader(s.serialize(
								document, $.kb()));

						BufferedReader gt = new BufferedReader(corpus
								.getGroundTruth(uri));

						List<String> gtList = getUris(gt);
						List<String> recoList = getUris(recos);
						float[] prf = calcPrecRecF1(gtList, recoList, k);
						System.out.println(String.format(
								"%1.5f\t%1.5f\t%1.5f\t%s\t%s", prf[0], prf[1],
								prf[2], gtList.size(), recoList.size()));
						return prf;
					}
				});

		BufferedWriter bw = new BufferedWriter(
				new FileWriter(
						"/home/adrian/Dokumente/diss/scoobie/results/dbpedia_en2/scoobie3.csv"));
		for (float[] prf : results) {
			bw.append(String.format("%1.5f\t%1.5f\t%1.5f", prf[0], prf[1],
					prf[2]));
			bw.newLine();
		}

		bw.close();

	}

	public static List<String> getUris(BufferedReader in) throws IOException {
		List<String> s = new ArrayList<String>();
		for (String line = in.readLine(); line != null; line = in.readLine()) {
			s.add(line.toLowerCase());
		}
		// System.out.println(s);
		return s;
	}

	public static float[] calcPrecRecF1(List<String> gt, List<String> recos,
			int k) {

		if (recos.size() == 0)
			return new float[] { 0.0f, 0.0f, 0.0f };

		List<String> recos2;

		if (recos.size() > k) {
			recos2 = recos.subList(0, k);
		} else {
			recos2 = recos;
		}

		Set<String> gtSet = new HashSet<String>(gt);
		Set<String> recoSet = new HashSet<String>(recos2);

		Set<String> truePositive = new HashSet<String>(recos2);
		truePositive.retainAll(gtSet);
		float precision = truePositive.size() / (float) recoSet.size();
		float recall = truePositive.size() / (float) gtSet.size();

		float f1 = (precision == 0.0f && recall == 0.0f) ? 0.0f : 2 * precision
				* recall / (precision + recall);
		return new float[] { precision, recall, f1 };
	}

}
/*
    Copyright (c) 2011, 
    Benjamin Adrian <benjamin.horak@gmail.com>
    German Research Center for Artificial Intelligence (DFKI) <info@dfki.de>
    
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

package de.dfki.km.perspecting.obie.postprocessor;

import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.File;
import java.net.URI;

import org.junit.BeforeClass;
import org.junit.Test;

import de.dfki.km.perspecting.obie.corpus.TextCorpus;
import de.dfki.km.perspecting.obie.corpus.WikinewsCorpus;
import de.dfki.km.perspecting.obie.model.Document;
import de.dfki.km.perspecting.obie.model.Scoobie;
import de.dfki.km.perspecting.obie.vocabulary.Language;
import de.dfki.km.perspecting.obie.vocabulary.MediaType;

public class TestRDFaSerializer {

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

	static Scoobie $ = null;
	private static RDFaSerializer rdfaGen;

	/******************* vocabulary ******************************************/
	private final String RDFS_LABEL = "http://www.w3.org/2000/01/rdf-schema#label";
	
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		try {
			
			final WikinewsCorpus corpus = new WikinewsCorpus(
					new File(
							"../corpora/wikinews/wikinews_text_labels.zip"),
					new TextCorpus(
							new File(
									"../corpora/wikinews/wikinews_text_labels.zip"),
							MediaType.ZIP, MediaType.HTML, Language.EN));

			$ = new Scoobie($DATABASE_DBPEDIA_en2, $DATABASE_SERVER_USER,
					$DATABASE_SERVER_PW, $DATABASE_SERVER_PORT,
					$DATABASE_SERVER_PC_4327, $CORPUS_HOME
							+ "en/wikipedia.type.maxent", corpus, new URI(
							"http://dbpedia.org"));
			rdfaGen = new RDFaSerializer();
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}
	}
	
	@Test
	public void testSimpleSentence() throws Exception {
		
		String sentence = "Angela Merkel is the German chancellor.";
		URI uri = new URI("file://example/test");
		String template = "SELECT * FROM NAMED <http://port-41xy?graph="
			+ $.kb().getUri()
			+ "&doc="
			+ uri.toString()
			+ "#recognized> WHERE {GRAPH <http://port-41xy?graph="
			+ $.kb().getUri()
			+ "&doc="
			+ uri.toString()
			+ "#recognized> {?s <" + RDFS_LABEL + "> ?o}}";

		Document document = $.pipeline().createDocument(sentence, new URI("file://example/test"), MediaType.TEXT, template, Language.EN);
		
		for(int step = 0; $.pipeline().hasNext(step); step = $.pipeline().execute(step, document)) {

		}
		
		BufferedReader r = new BufferedReader(rdfaGen.serialize(document, $.kb()));
		for(String line = r.readLine() ; line != null; line = r.readLine()) {
			System.out.println(line);
		}
		r.close();
		
	}
	
	@Test
	public void testHtmlSentence() throws Exception {
		
		String sentence = "<html><body>Angela <b>Merkel</b> is &nbsp; the German chancellor.</body></html>";
		URI uri = new URI("file://example/test");
		String template = "SELECT * FROM NAMED <http://port-41xy?graph="
			+ $.kb().getUri()
			+ "&doc="
			+ uri.toString()
			+ "#recognized> WHERE {GRAPH <http://port-41xy?graph="
			+ $.kb().getUri()
			+ "&doc="
			+ uri.toString()
			+ "#recognized> {?s <" + RDFS_LABEL + "> ?o}}";

		Document document = $.pipeline().createDocument(sentence, new URI("file://example/test"), MediaType.HTML, template, Language.EN);
		
		for(int step = 0; $.pipeline().hasNext(step); step = $.pipeline().execute(step, document)) {

		}
		
		BufferedReader r = new BufferedReader(rdfaGen.serialize(document, $.kb()));
		for(String line = r.readLine() ; line != null; line = r.readLine()) {
			System.out.println(line);
		}
		r.close();
		
	}

}

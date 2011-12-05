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

package de.dfki.km.perspecting.obie.model;

import java.net.URI;

import opennlp.tools.postag.POSModel;
import opennlp.tools.postag.POSTaggerME;

import org.postgresql.jdbc2.optional.PoolingDataSource;

import de.dfki.km.perspecting.obie.connection.KnowledgeBase;
import de.dfki.km.perspecting.obie.connection.PostgresKB;
import de.dfki.km.perspecting.obie.corpus.TextCorpus;
import de.dfki.km.perspecting.obie.preprocessor.RegexEntityRecognitionModel;
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
import de.dfki.km.perspecting.obie.transducer.model.MaxentEntityClassifierModel;
import de.dfki.km.perspecting.obie.transducer.model.disambiguation.AmbiguityResolver;
import de.dfki.km.perspecting.obie.transducer.model.disambiguation.DegreeBasedResolver;
import de.dfki.km.perspecting.obie.transducer.model.rating.CapacityBasedRating;
import de.dfki.km.perspecting.obie.transducer.model.rating.RatingMetric;
import de.dfki.km.perspecting.obie.vocabulary.Language;
import de.dfki.km.perspecting.obie.workflow.DummyTask;
import de.dfki.km.perspecting.obie.workflow.Pipeline;

public class Scoobie {
	
	private KnowledgeBase kb;
	private Pipeline pipeline;
	private PoolingDataSource pool;
	
	public Scoobie(String dbName, String dbUser, String dbPassword, int dbPort, String dbServerName,
			String classificationModel, TextCorpus corpus, URI uriKB) throws Exception {

		pool = new PoolingDataSource();

		pool.setUser(dbUser);
		pool.setPassword(dbPassword);
		pool.setPortNumber(dbPort);
		pool.setDatabaseName(dbName);
		pool.setServerName(dbServerName);
		pool.setMaxConnections(100);

		kb = new PostgresKB(pool.getConnection(), dbName, uriKB);
		pipeline = new Pipeline(kb);

		LanguageIdentification languageClassification = new LanguageIdentification(
				Language.EN);
		WordSegmenter wordTokenizer = new WordSegmenter();
		SentenceSegmenter sentenceTokenizer = new SentenceSegmenter();
		
		
		POSModel posModel = new POSModel(Scoobie.class.getResourceAsStream("pos/en/en-pos-maxent.bin"));
		POSTagging posTagger = new POSTagging(new POSTaggerME(posModel));

		ProperNameRecognition nounPhraseChunker = new ProperNameRecognition(new CRFNounPhraseChunkerModel(Scoobie.class.getResourceAsStream("npc/en/EN.crf")));

		SuffixArrayBuilder suffixArrayBuilder = new SuffixArrayBuilder(100);
		RDFLiteralSpotting namedEntityRecognizer = new RDFLiteralSpotting();
		InstanceRecognition instanceResolver = new InstanceRecognition();
		EntityDisambiguation instanceDisambiguator = new EntityDisambiguation(
				new AmbiguityResolver[] { new DegreeBasedResolver() });
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
		String[] regex = new String[] { DATE, FLOAT, POINT };
		RegexEntityRecognitionModel regexModel = new RegexEntityRecognitionModel(
				regex, kb);
		RegularStructuredEntityRecognition structuredEntityRecognizer = new RegularStructuredEntityRecognition(
				regexModel);



		DummyTask dummy = new DummyTask();

		pipeline.configure(languageClassification,
				wordTokenizer, sentenceTokenizer, posTagger, nounPhraseChunker,
				suffixArrayBuilder, namedEntityRecognizer, dummy, dummy,
				instanceResolver, dummy, dummy,
				dummy, dummy);
	}
	
	public KnowledgeBase kb() {
		return kb;
	}
	
	public Pipeline pipeline() {
		return pipeline;
	}
	
	public PoolingDataSource pool() {
		return pool;
	}

	
}



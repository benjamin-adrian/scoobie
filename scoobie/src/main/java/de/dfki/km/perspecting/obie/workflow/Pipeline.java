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

package de.dfki.km.perspecting.obie.workflow;

import java.io.BufferedReader;
import java.io.File;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URLEncoder;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import de.dfki.km.perspecting.obie.connection.KnowledgeBase;
import de.dfki.km.perspecting.obie.model.Document;
import de.dfki.km.perspecting.obie.model.FilterContext;
import de.dfki.km.perspecting.obie.vocabulary.Language;
import de.dfki.km.perspecting.obie.vocabulary.MediaType;

public class Pipeline {
	
	private final List<Transducer> pipeline = new ArrayList<Transducer>();

	private final KnowledgeBase kb;

	private final Logger log = Logger.getLogger(Pipeline.class.getName());;

	private String domain = "http://";

	public Pipeline(KnowledgeBase kb) {
		this.kb = kb;
		try {
			this.domain += InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}
	
	
	public void setDomain(String domain) {
		this.domain = "http://"+domain;
	}

	/**
	 * Configures the extraction pipeline with extractors.
	 * 
	 * @param textExtraction 0
	 * @param languageClassification 1
	 * @param wordTokenizer 2
	 * @param sentenceTokenizer 3 
	 * @param posTagger 4
	 * @param nounPhraseChunker 5
	 * @param suffixArrayBuilder 6 
	 * @param entityRecognizer 7
	 * @param regexRecognizer 8
	 * @param phraseClassifier 9
	 * @param subjectResolver 10
	 * @param subjectDisambiguator 11
	 * @param factEntailment 12
	 * @param relevanceRater 13
	 * @param factExtractor 14
	 */
	public void configure(
			Transducer languageClassification, 
			Transducer wordTokenizer,
			Transducer sentenceTokenizer, 
			Transducer posTagger,
			Transducer nounPhraseChunker, 
			Transducer suffixArrayBuilder,
			Transducer entityRecognizer, 
			Transducer regexRecognizer,
			Transducer phraseClassifier, 
			Transducer subjectResolver,
			Transducer subjectDisambiguator,
			Transducer factEntailment,
			Transducer relevanceRater, 
			Transducer factExtractor) {

		pipeline.add(languageClassification);
		pipeline.add(wordTokenizer);
		pipeline.add(sentenceTokenizer);
		pipeline.add(posTagger);
		pipeline.add(nounPhraseChunker);
		pipeline.add(suffixArrayBuilder);
		pipeline.add(entityRecognizer);
		pipeline.add(regexRecognizer);
		pipeline.add(phraseClassifier);
		pipeline.add(subjectResolver);
		pipeline.add(subjectDisambiguator);
		pipeline.add(factEntailment);
		pipeline.add(relevanceRater);
		pipeline.add(factExtractor);
	}

	public Transducer getTranducer(int step) {
		return pipeline.get(step);
	}

	public KnowledgeBase getKnowledgeBase() {
		return kb;
	}

	public Document createDocument(File file, URI uri, MediaType mimetype, String template, Language language) throws Exception {
		Document document = new Document(file, uri, mimetype, language);
		String baseURI = getBaseIEresultUri(document);
		document.setFilterContext(new FilterContext(kb.getUri(), baseURI + "predicted", baseURI + "recognized",  kb, template));
		
		return document;
	}
	
	public Document createDocument(String text, URI uri, MediaType mimetype, String template, Language language) throws Exception {
		Document document = new Document(text, uri, mimetype, language);
		String baseURI = getBaseIEresultUri(document);
		document.setFilterContext(new FilterContext(kb.getUri(), baseURI + "predicted", baseURI + "recognized",  kb, template));
		
		return document;
	}


	private String getBaseIEresultUri(Document document)
			throws UnsupportedEncodingException {
		String baseURI = this.domain+"?graph="+URLEncoder.encode(kb.getUri().toString(), "utf-8")+"&doc="+URLEncoder.encode(document.getUri().toString(), "utf-8")+ "#";
		return baseURI;
	}
	
	public Document createDocument(Reader reader, URI uri, MediaType mimetype, String template, Language language) throws Exception {
		
		BufferedReader br = new BufferedReader(reader);
		StringBuilder sb = new StringBuilder();
		for(String line = br.readLine(); line != null; line = br.readLine()) {
			sb.append(line);
			sb.append("\n");
		}
		return createDocument(sb.toString(), uri, mimetype, template, language);
	}



	/**
	 * 
	 * @param tranducer
	 * @param kb
	 * @param doc
	 */
	private void proceedStep(Transducer tranducer, KnowledgeBase kb,
			Document doc) {
		long start = System.currentTimeMillis();
		try {
			log.info("Start "
					+ tranducer.getClass().getSimpleName() + "...");
			tranducer.transduce(doc, kb);
		} catch (Throwable e) {
			log.log(Level.SEVERE, e.getMessage(), e);
		} finally {
			log.info("Finished "
					+ tranducer.getClass().getSimpleName() + ". It took "
					+ (System.currentTimeMillis() - start) + " ms");
		}
	}

	public int execute(int step, Document document) throws Exception {
		long start = System.currentTimeMillis();

		proceedStep(pipeline.get(step), kb, document);
		log.info("Finished executing pipeline. It took "
						+ (System.currentTimeMillis() - start) + " ms");
		return step + 1;
	}

	public boolean hasNext(int step) {
		return step < pipeline.size();
	}

}

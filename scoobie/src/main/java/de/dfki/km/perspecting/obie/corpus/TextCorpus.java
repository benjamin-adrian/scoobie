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

package de.dfki.km.perspecting.obie.corpus;

import gnu.trove.TDoubleFunction;
import gnu.trove.TIntDoubleHashMap;
import gnu.trove.TIntHashSet;
import gnu.trove.TIntIntHashMap;
import gnu.trove.TIntIntProcedure;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URI;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipFile;

import org.apache.commons.io.FileUtils;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.TermVector;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.search.IndexSearcher;

import de.dfki.km.perspecting.obie.model.Document;
import de.dfki.km.perspecting.obie.model.DocumentProcedure;
import de.dfki.km.perspecting.obie.model.SemanticEntity;
import de.dfki.km.perspecting.obie.model.Token;
import de.dfki.km.perspecting.obie.model.TokenSequence;
import de.dfki.km.perspecting.obie.transducer.RDFLiteralSpotting;
import de.dfki.km.perspecting.obie.vocabulary.Language;
import de.dfki.km.perspecting.obie.vocabulary.MediaType;
import de.dfki.km.perspecting.obie.workflow.Pipeline;

/**
 * A {@link TextCorpus} wraps a collection of text.
 * 
 * @author adrian
 * 
 */
public class TextCorpus {

	/**
	 * The location in file system of this corpus
	 */
	protected File corpus;

	protected ArrayList<InputStream> files = new ArrayList<InputStream>();
	
	protected MediaType corpusMediaType;

	protected Language language;

	private static final String SPACE = " ";

	protected final Logger log = Logger.getLogger(TextCorpus.class.getName());

	private MediaType corpusFileMediaType;

	/**
	 * Creates a new TextCorpus.
	 * 
	 * @param corpusFile
	 *            directory to contain text documents
	 * @param indexDir
	 *            directory to contain the lucene index
	 * @throws Exception 
	 */
	public TextCorpus(File corpusFile, MediaType corpusFileMediaType, MediaType corpusMediaType, Language language) throws Exception {
		this.corpus = corpusFile;
		this.corpusFileMediaType = corpusFileMediaType;
		this.corpusMediaType = corpusMediaType;
		this.language = language;
		log.info("creating corpus on " + corpusFile.getAbsolutePath());
	}

	@SuppressWarnings("unchecked")
	public List<?> forEach(DocumentProcedure<?> p)
			throws Exception {
		@SuppressWarnings("rawtypes")
		List l = new ArrayList();
		for (Entry<URI, InputStream> in : getEntries().entrySet()) {
			InputStreamReader reader = new InputStreamReader(in.getValue());
			log.info("processing entry: " + in.getKey().toString());
			l.add(p.process(reader, in.getKey()));
			reader.close();
		}
		return l;
	}

	/**
	 * Returns a Lucene index on this {@link TextCorpus}.
	 * 
	 * @param dir
	 *            The directory the index is stored.
	 * @param reindex
	 *            If <code>true</code>, an existing index will be re-created.
	 * @return Access to the Lucene index.
	 * 
	 * @throws Exception
	 */
	public IndexSearcher getLuceneIndex(File dir, boolean reindex)
			throws Exception {

		if (dir.exists()) {
			if (reindex) {
				FileUtils.deleteDirectory(dir);
				log.info("deleted directory: " + dir);
			} else {
				return new IndexSearcher(dir.getAbsolutePath());
			}
		}

		dir.mkdirs();
		log.info("created directory: " + dir);

		final WhitespaceAnalyzer analyser = new WhitespaceAnalyzer();

		final IndexWriter indexWriter = new IndexWriter(dir, analyser, true,
				MaxFieldLength.LIMITED);
		forEach(new DocumentProcedure<String>() {
			@Override
			public String process(Reader doc, URI uri) throws Exception {
				org.apache.lucene.document.Document document = new org.apache.lucene.document.Document();
				document.add(new Field("text", doc, TermVector.YES));
				indexWriter.addDocument(document, analyser);
				log.fine("indexes: " + document);
				return uri.toString();
			}
		});
		log.info("indexed: " + indexWriter.numDocs() + " documents");

		indexWriter.commit();
		indexWriter.close();

		return new IndexSearcher(dir.getAbsolutePath());
	}
	
	
	@SuppressWarnings("unchecked")
	public TIntDoubleHashMap getDocumentFrequency(final Pipeline pipe) throws Exception {

		// calculate property frequency per document
		final List<TIntIntHashMap> results = (List<TIntIntHashMap>) forEach(new DocumentProcedure<TIntIntHashMap>() {

					@Override
					public TIntIntHashMap process(final Reader file,
							final URI uri) throws Exception {

						final Document document = pipe.createDocument(file,
								uri, getMediatype(), "SELECT * WHERE {?s ?p ?o}", getLanguage());

						final TIntIntHashMap stats = new TIntIntHashMap();
						for (int step = 0; pipe.hasNext(step) && step < 8; step = pipe
								.execute(step, document)) {
							if (step > 0
									&& pipe.getTranducer(step - 1).getClass()
											.equals(RDFLiteralSpotting.class)) {
								for (final TokenSequence<SemanticEntity> se : document
										.getRetrievedPropertyValues()) {
									stats.adjustOrPutValue(se.getValue()
											.getPropertyIndex(), 1, 1);
								}
								break;
							}
						}
						return stats;
					}
				});

		final TIntDoubleHashMap propertyIDF = new TIntDoubleHashMap();

		for (TIntIntHashMap indexedDoc : results) {
			indexedDoc.forEachEntry(new TIntIntProcedure() {
				@Override
				public boolean execute(int property, int value) {
					propertyIDF.adjustOrPutValue(property, 1.0, 1.0);
					return true;
				}
			});
		}

		propertyIDF.transformValues(new TDoubleFunction() {
			@Override
			public double execute(double value) {

				double idf = ((double) results.size()) / (value + 1);
				return idf;
			}
		});

		return propertyIDF;
	}
	

	public LabeledTextCorpus labelRDFTypes(final File corpus,
			final Pipeline pipeline, final String template) throws Exception {

		final BufferedWriter writer = new BufferedWriter(new FileWriter(corpus));

		this.forEach(new DocumentProcedure<String>() {
			@Override
			public String process(Reader doc, URI uri) throws Exception {

				Document document = pipeline.createDocument(doc, uri,
						corpusMediaType, template, language);

				for (int step = 0; pipeline.hasNext(step); step = pipeline
						.execute(step, document))
					;

				TIntHashSet sentenceBoundaries = new TIntHashSet();
				for (TokenSequence<Integer> sentence : document.getSentences()) {
					sentenceBoundaries.add(sentence.getEnd());
				}

				for (Token token : document) {
					String word = token.toString();
					String pos = token.getPartOfSpeechTag();
					String phrase = token.getNounPhraseTag();
					int label = -1;

					int[] types = token.getTypes(0.0).toArray();
					if (types.length > 0) {
						label = pipeline.getKnowledgeBase().getCluster(types);
						// System.out.println(word + " " + kb.getURI(label));
					}

					// int[] subjects = token.getSubjects().toArray();
					// if (subjects.length > 0) {
					// System.out.println(word + " " +
					// Arrays.toString(subjects));
					// }
					writer.append(word);
					writer.append(SPACE);
					writer.append(pos);
					writer.append(SPACE);
					writer.append(phrase);
					writer.append(SPACE);

					if (label > 0) {
						writer.append(Integer.toString(label));
					} else {
						writer.append(LabeledTextCorpus.OUTSIDE_ANY_LABEL);
					}

					writer.newLine();

					if (sentenceBoundaries.contains(token.getEnd())) {
						writer.newLine();
					}
				}

				writer.flush();
				return uri.toString();
			}

		});
		writer.close();
		return new LabeledTextCorpus(corpus, MediaType.TEXT, this);

	}

	/**
	 * @return list of files in corpus
	 * @throws IOException 
	 * @throws ZipException 
	 */
	@SuppressWarnings("unchecked")
	protected HashMap<URI, InputStream> getEntries() throws Exception {

		HashMap<URI, InputStream> entries = new HashMap<URI, InputStream>();
		
		if(corpusFileMediaType == MediaType.ZIP) {
			ZipFile zippedCorpusDir = new ZipFile(corpus);
			Enumeration<? extends ZipEntry> zipEntries = zippedCorpusDir.entries();
			while(zipEntries.hasMoreElements()) {
				ZipEntry zipEntry = zipEntries.nextElement();
				if(!zipEntry.isDirectory()) {
					
					String uriValue = corpus.toURI().toString()+"/";
					String entryName = zipEntry.getName();
					uriValue += URLEncoder.encode(entryName, "utf-8");
	
					entries.put(new URI(uriValue), zippedCorpusDir.getInputStream(zipEntry));
				}
			}
		} else if(corpusFileMediaType == MediaType.DIRECTORY) {
			for(File f : corpus.listFiles()) {
				entries.put(f.toURI(), new FileInputStream(f));
			}
		}
		
		return entries;
	}

	public File getCorpus() {
		return corpus;
	}

	public Language getLanguage() {
		return language;
	}

	public MediaType getMediatype() {
		return corpusMediaType;
	}
	
	public MediaType getCorpusFileMediaType() {
		return corpusFileMediaType;
	}

}

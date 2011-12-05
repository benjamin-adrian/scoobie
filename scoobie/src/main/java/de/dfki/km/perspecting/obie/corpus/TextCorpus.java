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

import gnu.trove.TIntHashSet;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.Reader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.logging.Logger;

import org.apache.commons.io.FileUtils;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.TermVector;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.search.IndexSearcher;

import de.dfki.km.perspecting.obie.model.Document;
import de.dfki.km.perspecting.obie.model.DocumentProcedure;
import de.dfki.km.perspecting.obie.model.Token;
import de.dfki.km.perspecting.obie.model.TokenSequence;
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

	protected MediaType mediatype;

	protected Language language;

	private static final String SPACE = " ";

	protected final Logger log = Logger.getLogger(TextCorpus.class.getName());

	/**
	 * Creates a new TextCorpus.
	 * 
	 * @param corpusDir
	 *            directory to contain text documents
	 * @param indexDir
	 *            directory to contain the lucene index
	 */
	public TextCorpus(File corpusDir, MediaType mediatype, Language language) {
		this.corpus = corpusDir;
		this.mediatype = mediatype;
		this.language = language;
	}


	@SuppressWarnings("unchecked")
	public List<?> forEach(DocumentProcedure<?> p) throws Exception {
		return forEach(p, corpus);
	}
	
	public List<?> forEach(DocumentProcedure<?> p, File directory) throws Exception {
		List l = new ArrayList();
		for (File f : getFiles(directory)) {
			log.info("processing file: " + f.getName());
			FileReader reader = new FileReader(f);
			l.add(p.process(reader, f.toURI()));
			reader.close();
		}
		return l;
	}

	/**
	 * Returns a Lucene index on this {@link TextCorpus}.
	 * 
	 * @param dir The directory the index is stored.
	 * @param reindex If <code>true</code>, an existing index will be re-created.
	 * @return Access to the Lucene index.
	 * 
	 * @throws Exception
	 */
	public IndexSearcher getLuceneIndex(File dir, boolean reindex) throws Exception {

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

	public LabeledTextCorpus labelRDFTypes(final File corpus,
			final Pipeline pipeline, final String template) throws Exception {

		final BufferedWriter writer = new BufferedWriter(new FileWriter(corpus));

		this.forEach(new DocumentProcedure<String>() {
			@Override
			public String process(Reader doc, URI uri) throws Exception {

				Document document = pipeline.createDocument(doc, uri,
						mediatype, template, language);

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
		return new LabeledTextCorpus(corpus, this);

	}

	/**
	 * @return list of files in corpus
	 */
	@SuppressWarnings("unchecked")
	protected Collection<File> getFiles(File corpus) {

		if (corpus.isDirectory()) {
			Collection<File> files = FileUtils.listFiles(corpus, null, false);
			return files;
		} else {
			return Arrays.asList(new File[] { corpus });
		}
	}

	public File getCorpus() {
		return corpus;
	}

	public Language getLanguage() {
		return language;
	}

	public MediaType getMediatype() {
		return mediatype;
	}

}

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

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Map.Entry;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;

import de.dfki.km.perspecting.obie.transducer.model.SuffixArray;
import de.dfki.km.perspecting.obie.vocabulary.Language;
import de.dfki.km.perspecting.obie.vocabulary.MediaType;
import edu.uci.ics.jung.graph.DirectedGraph;
import gnu.trove.TIntHashSet;
import gnu.trove.TIntObjectHashMap;

/**
 * The {@link Document} is the base document representation along the extraction
 * pipeline.
 * 
 * @author adrian
 * 
 */
public class Document implements Iterable<Token> {

	private URI uri;
	private String plainTextContent;
	private String content;

	private MediaType mimeType = MediaType.TEXT;
	private Language language = Language.UNKNOWN;

	private DataSheet data = new DataSheet();

	private final Logger log = Logger.getLogger(Document.class.getName());

	public Document(String content, URI uri, MediaType mimeType,
			Language language) throws Exception {
		this.content = content;
		this.uri = uri;
		this.mimeType = mimeType;
		this.language = language;
		normalizeContent();
	}

	public Document(File file, URI uri, MediaType mimeType, Language language) throws Exception {
		this(FileUtils.readFileToString(file), uri, mimeType, language);
	}

	private void normalizeContent() throws Exception {

		switch (mimeType) {

		case HTML:
			plainTextContent = extractPlainTextFromHtml(content);
			break;
		case XHTML:
			plainTextContent = extractPlainTextFromHtml(content);
			break;
		case TEXT:
		default:
			plainTextContent = content;
		}

	}

	/***************************************************************************
	 * Gets the pure plain text out of a html text. All html tags are replaced
	 * by spaces. To do so, the head is replaced, all remaining javascript tags
	 * (including the content) and finally all remaining html tags. Thus,
	 * absolute positioning is possible.
	 * 
	 * @param text
	 *            content of the html document as text
	 * @return text where all html was replaced by spaces
	 */
	private String extractPlainTextFromHtml(String text) {
		Collection<Pattern> patterns = new ArrayList<Pattern>(3);
		// Delete the head, then all remaining javascript items that might exist
		// in the body, then all remaining html tags.
		patterns.add(Pattern.compile("<head.*/head>", Pattern.CASE_INSENSITIVE
				| Pattern.UNICODE_CASE | Pattern.DOTALL));
		// .*? makes it non greedy -> take the shortes match
		// DOTALL does also include new lines
		patterns.add(Pattern.compile("<script.*?/script>",
				Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE
						| Pattern.DOTALL));
		patterns.add(Pattern.compile("<.+?>", Pattern.CASE_INSENSITIVE
				| Pattern.UNICODE_CASE));
		StringBuffer s = new StringBuffer(text);

		// Go for all patterns.
		for (Pattern p : patterns) {
			Matcher matcher = p.matcher(s);

			// As long as the matcher finds another occurance of the pattern we
			// replace it by the same number of spaces but keep new lines.
			while (matcher.find())
				s.replace(matcher.start(), matcher.end(), matcher.group()
						.replaceAll(".", " "));
		}
		return s.toString();
	}

	/**
	 * @return the uri
	 */
	public URI getUri() {
		return uri;
	}

	/**
	 * @param uri
	 *            the uri to set
	 */
	public void setUri(URI uri) {
		this.uri = uri;
	}

	/**
	 * @return the plainTextContent
	 */
	public final String getPlainTextContent() {
		return plainTextContent;
	}
	
	public String getContent() {
		return content;
	}

	/**
	 * @return the mimeType
	 */
	public MediaType getMimeType() {
		return mimeType;
	}

	/**
	 * @return the language
	 */
	public Language getLanguage() {
		return language;
	}

	/**
	 * @param language
	 *            the language to set
	 */
	public void setLanguage(Language language) {
		this.language = language;
	}

	/**
	 * The Literals-Subject graph contains links from literals recognized in a
	 * text and RDF subjects of triples of these subject having the literal
	 * value as object.
	 */
	private DirectedGraph<Integer, RDFEdge> graph;

	private DirectedGraph<Integer, RDFEdge> predictionGraph;
	private String template;

	private FilterContext filterContext;

	private Set<Set<Integer>> literalSubjectPairs;

	private SuffixArray suffixArray;

	private DoubleMatrix ambiguityScores;
	private DoubleMatrix relevanceScores;

	public DoubleMatrix getAmbiguityScores() {
		return ambiguityScores;
	}

	public void setAmbiguityScores(DoubleMatrix ambiguityScores) {
		this.ambiguityScores = ambiguityScores;
	}

	public FilterContext getFilterContext() {
		return filterContext;
	}

	public Set<Set<Integer>> getLiteralSubjectPairs() {
		return literalSubjectPairs;
	}

	public DataSheet getData() {
		return data;
	}

	public DirectedGraph<Integer, RDFEdge> getGraph() {
		return graph;
	}

	public void setGraph(DirectedGraph<Integer, RDFEdge> graph) {
		this.graph = graph;
	}

	public DirectedGraph<Integer, RDFEdge> getPredictionGraph() {
		return predictionGraph;
	}

	public void setPredictionGraph(DirectedGraph<Integer, RDFEdge> graph) {
		this.predictionGraph = graph;
	}

	public void removeUnresolvedSubjects(int[] subjects) {

		TIntHashSet _subjects = new TIntHashSet(subjects);

		for (int tokenIndex : data.getIntegerKeys(TokenSequence.SUBJECT)) {
			List<SemanticEntity> values = data.get(TokenSequence.SUBJECT,
					tokenIndex);
			Set<Integer> indexes = new HashSet<Integer>();
			int i = 0;
			for (SemanticEntity value : values) {

				int subject = value.getSubjectIndex();

				if (_subjects.contains(subject)) {
					indexes.add(i);
				}
				i++;
			}

			for (int j : indexes) {
				log.fine("removed entity: " + values.get(j));
				values.set(j, null);
			}

			while (values.remove(null))
				;

		}

	}

	/**
	 * Returns all RDF subjects with matching literal property values in text.
	 */
	public List<TokenSequence<SemanticEntity>> getResolvedSubjects() {

		// collection that will be returned as result
		List<TokenSequence<SemanticEntity>> entities = new ArrayList<TokenSequence<SemanticEntity>>();

		HashMap<Integer, TokenSequence<SemanticEntity>> map = new HashMap<Integer, TokenSequence<SemanticEntity>>();

		for (int tokenIndex : data.getIntegerKeys(TokenSequence.SUBJECT)) {
			List<SemanticEntity> values = data.get(TokenSequence.SUBJECT,
					tokenIndex);
			assert values != null; // when does this occur?
			for (SemanticEntity value : values) {

				int subject = value.getSubjectIndex();
				if (value.getPosition().equals("B")) {
					TokenSequence<SemanticEntity> entity = map.get(subject);
					if (entity != null) {
						entities.add(map.remove(subject));
					}
					entity = new TokenSequence<SemanticEntity>(value);
					entity.addToken(new Token(tokenIndex, this));
					map.put(subject, entity);
				} else {
					map.get(subject).addToken(new Token(tokenIndex, this));
				}
			}

		}
		entities.addAll(map.values());

		return entities;
		//		
		//		
		//		
		//		
		//		
		// List<TokenSequence<SemanticEntity>> entities = new
		// ArrayList<TokenSequence<SemanticEntity>>();
		// TokenSequence<SemanticEntity> entity = null;
		//
		// for (int tokenIndex :
		// this.data.getIntegerKeys(TokenSequence.SUBJECT)) {
		// List<SemanticEntity> value = this.data.get(TokenSequence.SUBJECT,
		// tokenIndex);
		// for (SemanticEntity e : value) {
		// if (e.getPosition().equals("B")) { // equal for all entries.
		// if (entity != null) {
		// entities.add(entity);
		// }
		// entity = new TokenSequence<SemanticEntity>(e);
		// entity.addToken(new Token(tokenIndex, this));
		// } else {
		// assert entity != null;
		// entity.addToken(new Token(tokenIndex, this));
		// }
		// }
		// }
		// if (entity != null) {
		// entities.add(entity);
		// entity = null;Set
		// }
		//
		// return entities;
	}

	/**
	 * Returns for a given set of types the best rorrelating cluster label or
	 * null.
	 * 
	 * @param clusters
	 *            mapping between RDF type and assigned cluster label.
	 * @param types
	 *            the types of the RDF subject(s)
	 */
	private Integer getClusterLabel(Map<Integer, Integer> clusters,
			Set<Integer> types) {

		Set<Integer> labels = new HashSet<Integer>();

		for (Integer t : types) {
			labels.add(clusters.get(t));
		}

		if (labels.size() == 1) {
			return labels.iterator().next();
		}
		return null;

	}

	/**
	 * 
	 * @param clusters
	 * @return
	 */
	public List<TokenSequence<SemanticEntity>> getUnambiguoslyTypedEntities(
			Map<Integer, Integer> clusters, Set<Integer> rootLabels) {

		TIntObjectHashMap<List<TokenSequence<SemanticEntity>>> m = new TIntObjectHashMap<List<TokenSequence<SemanticEntity>>>();

		List<TokenSequence<SemanticEntity>> out = new ArrayList<TokenSequence<SemanticEntity>>();

		// aggregate equally positioned TokenSequence on different
		// SemanticEntities
		for (TokenSequence<SemanticEntity> e : getResolvedSubjects()) {
			List<TokenSequence<SemanticEntity>> se = m.get(e.getStart());
			if (se == null) {
				se = new ArrayList<TokenSequence<SemanticEntity>>();
				m.put(e.getStart(), se);
			}
			se.add(e);
		}

		// filter unambiguous entities
		for (int key : m.keys()) {
			HashSet<Integer> types = new HashSet<Integer>();
			TokenSequence<SemanticEntity> phrase = null;

			for (TokenSequence<SemanticEntity> se : m.get(key)) {
				phrase = se;
				for (TIntDoubleTuple t : se.getValue().getTypeIndex()) {
					types.add(t.key);
				}
			}

			types.removeAll(rootLabels);

			Integer label = getClusterLabel(clusters, types);

			if (label != null) {

				SemanticEntity semEnt = new SemanticEntity();
				semEnt.addTypeIndex(label, 1.0);

				TokenSequence<SemanticEntity> ts = new TokenSequence<SemanticEntity>(
						semEnt);
				// copy tokens of phrase
				for (Token t : phrase.getTokens()) {
					ts.addToken(t);
				}
				out.add(ts);
			}
		}
		return out;
	}

	public List<TokenSequence<SemanticEntity>> getRetrievedPropertyValues() {
		List<TokenSequence<SemanticEntity>> entities = new ArrayList<TokenSequence<SemanticEntity>>();

		HashMap<String, TokenSequence<SemanticEntity>> map = new HashMap<String, TokenSequence<SemanticEntity>>();

		for (int tokenIndex : this.data.getIntegerKeys(TokenSequence.PROPERTY)) {
			List<SemanticEntity> values = this.data.get(TokenSequence.PROPERTY,
					tokenIndex);
			if (values != null) {
				for (SemanticEntity value : values) {

					String key = Integer.toString(value.getPropertyIndex())
							+ Integer.toString(value.getLiteralValueIndex());

					if (value.getPosition().equals("B")) {
						TokenSequence<SemanticEntity> entity = map.get(key);
						if (entity != null) {
							entities.add(map.remove(key));
						}
						entity = new TokenSequence<SemanticEntity>(value);
						entity.addToken(new Token(tokenIndex, this));
						map.put(key, entity);
					} else {
						map.get(key).addToken(new Token(tokenIndex, this));
					}
				}
			} else {
				entities.addAll(map.values());
				map.clear();
			}
		}
		entities.addAll(map.values());

		return entities;
	}

	public List<TokenSequence<SemanticEntity>> getEntityTypes() {
		List<TokenSequence<SemanticEntity>> entities = new ArrayList<TokenSequence<SemanticEntity>>();

		HashMap<Integer, TokenSequence<SemanticEntity>> map = new HashMap<Integer, TokenSequence<SemanticEntity>>();

		for (int tokenIndex : this.data.getIntegerKeys(TokenSequence.TYPE)) {
			List<SemanticEntity> values = this.data.get(TokenSequence.TYPE,
					tokenIndex);
			if (values != null) {
				for (SemanticEntity value : values) {
					int property = value.getPropertyIndex();
					if (value.getPosition().equals("B")) {
						TokenSequence<SemanticEntity> entity = map
								.get(property);
						if (entity != null) {
							entities.add(map.remove(property));
						}
						entity = new TokenSequence<SemanticEntity>(value);
						entity.addToken(new Token(tokenIndex, this));
						map.put(property, entity);
					} else {
						map.get(property).addToken(new Token(tokenIndex, this));
					}
				}
			} else {
				entities.addAll(map.values());
				map.clear();
			}
		}
		entities.addAll(map.values());

		return entities;
	}

	public List<TokenSequence<String>> getNounPhrases() {

		List<TokenSequence<String>> phrases = new ArrayList<TokenSequence<String>>();

		TokenSequence<String> phrase = null;

		for (int tokenIndex : this.data
				.getIntegerKeys(TokenSequence.NOUN_PHRASE)) {
			String tag = this.data.get(TokenSequence.NOUN_PHRASE, tokenIndex);

			// TODO: Fix CRF: Transitions O -> I-NP are never allowed!
			if (tag.equals("I-NP")) {

				// If there has been no B-NP before, we change this I-NP to a
				// B-NP and proceed with it as it was a B-NP before.
				if (phrase == null)
					tag = "B-NP";
				else
					phrase.addToken(new Token(tokenIndex, this));
			}

			// Caution: Don't use an else here, since we need to check for B-NP
			// even though we already found an I-NP (see comment above).
			if (tag.equals("B-NP")) {
				if (phrase != null) {
					phrases.add(phrase);
				}
				phrase = new TokenSequence<String>("B-NP");
				phrase.addToken(new Token(tokenIndex, this));
			}
		}
		if (phrase != null) {
			phrases.add(phrase);
			phrase = null;
		}

		return phrases;
	}

	public List<TokenSequence<Integer>> getSentences() {
		TreeMap<Integer, TokenSequence<Integer>> sentences = new TreeMap<Integer, TokenSequence<Integer>>();

		for (Entry<String, Integer> token : this.data
				.integerEntries(TokenSequence.SENTENCE)) {

			int start = Integer.parseInt(token.getKey());

			TokenSequence<Integer> sentence = sentences.get(token.getValue());
			if (sentence == null) {
				sentence = new TokenSequence<Integer>(token.getValue());
				sentences.put(token.getValue(), sentence);
			}
			sentence.addToken(new Token(start, this));

		}

		return new ArrayList<TokenSequence<Integer>>(sentences.values());
	}

	public TokenSequence<Integer> getSentence(int index) {
		TokenSequence<Integer> sentence = new TokenSequence<Integer>(index);
		for (Entry<String, Integer> s : this.data
				.integerEntries(TokenSequence.SENTENCE)) {
			int start = Integer.parseInt(s.getKey());
			if (s.getValue() == index) {
				sentence.addToken(new Token(start, this));
			}
		}
		return sentence;
	}

	/**
	 * @return the tokens
	 */
	public List<Token> getTokens() {
		TreeSet<Token> tokens = new TreeSet<Token>();

		for (String key : this.data.getKeys(TokenSequence.TOKEN)) {
			tokens.add(new Token(Integer.parseInt(key), this));
		}
		return new ArrayList<Token>(tokens);
	}

	/**
	 * @return the tokens
	 */
	public List<Token> getTokens(int start, int end) {
		ArrayList<Token> list = new ArrayList<Token>();
		ArrayList<Integer> keys = new ArrayList<Integer>(this.data
				.getIntegerKeys(TokenSequence.TOKEN));

		for (int i = 0; i < keys.size() && keys.get(i) <= start; i++) {

			if (keys.get(i) == start) {

				Token t;

				int i1 = i;
				do {
					t = new Token(keys.get(i1), this);
					if (t.getEnd() < end) {
						list.add(t);
					}

					if (t.getEnd() == end) {
						list.add(t);
					}
					i1++;
				} while (t.getEnd() <= end);
			}

		}

		if (list.get(list.size() - 1).getEnd() != end) {
			list.clear();
		}

		return list;
	}

	public String getTemplate() {
		return template;
	}

	/**
	 * @param template
	 *            the template to set
	 */
	public void setTemplate(String template) {
		this.template = template;
	}

	public void setFilterContext(FilterContext filterContext) {
		this.filterContext = filterContext;
	}

	public void setLiteralsSubjectPairs(Set<Set<Integer>> literalSubjectPairs) {
		this.literalSubjectPairs = literalSubjectPairs;

	}

	public void setSuffixArray(SuffixArray suffixArray) {
		this.suffixArray = suffixArray;
	}

	public SuffixArray getSuffixArray() {
		return suffixArray;
	}

	@Override
	public Iterator<Token> iterator() {
		return getTokens().iterator();
	}

	public void setRelevanceScores(DoubleMatrix matrix) {
		this.relevanceScores = matrix;
	}

	public DoubleMatrix getRelevanceScores() {
		return relevanceScores;
	}
}

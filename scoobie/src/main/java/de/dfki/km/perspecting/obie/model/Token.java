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

import gnu.trove.TIntHashSet;

import java.util.ArrayList;
import java.util.List;

public class Token implements Comparable<Token> {

	private final int tokenIndex;

	private final Document documentData;

	public Token(final int start, final Document documentData) {
		this.tokenIndex = start;
		this.documentData = documentData;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {

		if (obj instanceof Token)
			return this.equals((Token) obj);
		else
			return false;
	}

	public boolean equals(Token obj) {
		return tokenIndex == obj.tokenIndex;
	}

	@Override
	public int hashCode() {
		return tokenIndex;
	}

	public int getStart() {
		return tokenIndex;
	}

	public int getEnd() {
		return ((Integer)documentData.getData().get(TokenSequence.TOKEN, tokenIndex));
	}

	// public Map<String, Set<String>> getFeatureLine(String... features) {
	// Map<String, Set<String>> featureLine = new HashMap<String,
	// Set<String>>();
	// for (String feature : features) {
	// HashSet<String> values = new HashSet<String>();
	// if (feature.equals(TokenSequence.TOKEN)) {
	// values.add(toString());
	// } else {
	// List<SemanticEntity> o = documentData.getData().get(feature, tokenIndex);
	// if(o != null) {
	// for (SemanticEntity e : o) {
	// values.add(e.g);
	// }
	// } else if (o != null){
	// values.add(o.toString());
	// }
	// }
	// featureLine.put(feature, values);
	//
	// }
	//
	// return featureLine;
	// }

	public void setPOS(String pos) {
		documentData.getData().add(TokenSequence.POS, tokenIndex, pos);
	}

	public void setSentence(int sentence) {
		documentData.getData()
				.add(TokenSequence.SENTENCE, tokenIndex, sentence);
	}

	public void setNounPhraseTag(String npTag) {
		documentData.getData()
				.add(TokenSequence.NOUN_PHRASE, tokenIndex, npTag);
	}

	public String getNounPhraseTag() {
		return documentData.getData()
				.get(TokenSequence.NOUN_PHRASE, tokenIndex);
	}

	public TokenSequence<Integer> getSentence() {
		int sentenceIndex = ((Integer)documentData.getData().get(TokenSequence.SENTENCE,
				tokenIndex));
		return documentData.getSentence(sentenceIndex);
	}

	public String getPartOfSpeechTag() {
		return documentData.getData().get(TokenSequence.POS, tokenIndex);
	}

	public final String getTextSource() {
		return documentData.getPlainTextContent();
	}

	public SemanticEntity addProperty(String position, int literalValue,
			int rdfProperty) {
		SemanticEntity entity = new SemanticEntity();
		entity.setLiteralValueIndex(literalValue);
		entity.setPropertyIndex(rdfProperty);
		entity.setPosition(position);

		List<SemanticEntity> plainProperties = documentData.getData().get(
				TokenSequence.PROPERTY, tokenIndex);
		if (plainProperties == null) {
			plainProperties = new ArrayList<SemanticEntity>();
			documentData.getData().add(TokenSequence.PROPERTY, tokenIndex,
					plainProperties);
		}
		plainProperties.add(entity);
		return entity;
	}

	public SemanticEntity addRegexMatch(String position, String regex) {
		SemanticEntity entity = new SemanticEntity();
		entity.setRegex(regex);
		entity.setPosition(position);

		List<SemanticEntity> regexProperties = documentData.getData().get(
				TokenSequence.REGEX, tokenIndex);
		if (regexProperties == null) {
			regexProperties = new ArrayList<SemanticEntity>();
			documentData.getData().add(TokenSequence.REGEX, tokenIndex,
					regexProperties);
		}
		regexProperties.add(entity);
		return entity;
	}

	public SemanticEntity addSubject(String position, SemanticEntity entity,
			int subjectUriIndex, String subjectURI) {
		SemanticEntity e = new SemanticEntity();
		e.setLiteralValueIndex(entity.getLiteralValueIndex());
		e.setPropertyIndex(entity.getPropertyIndex());
		e.setPosition(position);
		e.setSubjectIndex(subjectUriIndex);
		e.setSubjectURI(subjectURI);

		List<SemanticEntity> subjects = documentData.getData().get(
				TokenSequence.SUBJECT, tokenIndex);
		if (subjects == null) {
			subjects = new ArrayList<SemanticEntity>();
			documentData.getData().add(TokenSequence.SUBJECT, tokenIndex,
					subjects);
		}
		subjects.add(e);
		return e;
	}

	public SemanticEntity addType(SemanticEntity entity, int type,
			double propability) {

		SemanticEntity e = new SemanticEntity();
		e.setLiteralValueIndex(entity.getLiteralValueIndex());
		e.setPropertyIndex(entity.getPropertyIndex());
		e.setPosition(entity.getPosition());
		e.setSubjectIndex(entity.getSubjectIndex());
		e.setSubjectURI(entity.getSubjectURI());
		e.addTypeIndex(type, propability);

		List<SemanticEntity> types = documentData.getData().get(
				TokenSequence.TYPE, tokenIndex);
		if (types == null) {
			types = new ArrayList<SemanticEntity>();
			documentData.getData().add(TokenSequence.TYPE, tokenIndex, types);
		}
		types.add(e);
		return e;
	}

	public TIntHashSet getTypes(double propability) {

		TIntHashSet set = new TIntHashSet();

		List<SemanticEntity> typeList = documentData.getData().get(
				TokenSequence.TYPE, tokenIndex);
		if (typeList != null)
			for (SemanticEntity e : typeList) {
				for (TIntDoubleTuple t : e.getTypeIndex()) {
					if (t.value >= propability)
						set.add(t.key);
				}
			}

		return set;
	}

	public TIntHashSet getPredictedTypes(double propability) {

		TIntHashSet set = new TIntHashSet();

		List<SemanticEntity> typeList = documentData.getData().get(
				TokenSequence.TYPE, tokenIndex);
		if (typeList != null)
			for (SemanticEntity e : typeList) {
				if (e.getSubjectIndex() == -1) {
					for (TIntDoubleTuple t : e.getTypeIndex()) {
						if (t.value >= propability)
							set.add(t.key);
					}
				}
			}

		return set;
	}

	public List<String> getURIReferences() {

		ArrayList<String> set = new ArrayList<String>();

		List<SemanticEntity> typeList = documentData.getData().get(
				TokenSequence.SUBJECT, tokenIndex);
		if (typeList != null) {
			for (SemanticEntity e : typeList) {
				set.add(e.getSubjectURI());
			}
		}
		return set;
	}

	public TIntHashSet getSubjects() {
		TIntHashSet set = new TIntHashSet();

		List<SemanticEntity> typeList = documentData.getData().get(
				TokenSequence.SUBJECT, tokenIndex);
		if (typeList != null) {
			for (SemanticEntity e : typeList) {
				set.add(e.getSubjectIndex());
			}
		}
		return set;
	}
	
	public List<SemanticEntity> getAnnotations() {
		List<SemanticEntity> list = documentData.getData().get(
				TokenSequence.SUBJECT, tokenIndex);
		if (list == null) {
			list = new ArrayList<SemanticEntity>();
		}
		return list;
	}

	public SemanticEntity addTypes(SemanticEntity entity, int[] types,
			double propability) {

		SemanticEntity e = new SemanticEntity();
		e.setLiteralValueIndex(entity.getLiteralValueIndex());
		e.setPropertyIndex(entity.getPropertyIndex());
		e.setPosition(entity.getPosition());
		e.setSubjectIndex(entity.getSubjectIndex());
		e.setSubjectURI(entity.getSubjectURI());

		for (int type : types) {
			e.addTypeIndex(type, propability);
		}

		List<SemanticEntity> typeList = documentData.getData().get(
				TokenSequence.TYPE, tokenIndex);
		if (typeList == null) {
			typeList = new ArrayList<SemanticEntity>();
			documentData.getData()
					.add(TokenSequence.TYPE, tokenIndex, typeList);
		}
		typeList.add(e);
		return e;
	}

	@Override
	public final String toString() {
		return documentData.getPlainTextContent().substring(
				tokenIndex, getEnd());
	}

	@Override
	public int compareTo(Token o) {
		return tokenIndex - o.tokenIndex;
	}
}
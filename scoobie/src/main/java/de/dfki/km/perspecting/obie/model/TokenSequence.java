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

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

/**
 * Objects of type {@link TokenSequence} describe annotations about a text
 * sequence.
 * 
 * @author adrian
 * 
 */
public class TokenSequence<T> implements
		Comparable<TokenSequence<T>> {

	public static final String TOKEN = "token";
	public static final String SENTENCE = "sent";
	public static final String NOUN_PHRASE = "np";
	public static final String POS = "pos";
	
	public static final String SUBJECT = "subject";
	public static final String PROPERTY = "property";
	public static final String REGEX = "regex";
	public static final String TYPE = "type";
	
//	public static final String PLAIN_PROPERTY = "plain_property";
//	public static final String PLAIN_TYPE = "plain_type";
//	public static final String PLAIN_SUBJECT = "plain_subject";
//	public static final String PREDICTED_TYPE = "predicted_type";
	
	
//	public static final String IS_SENTENCE = "is_sentence";
//	public static final String IS_STRUCTURED_ENTITY = "is_structured_entity";

//	public static final String STRING = "string";

	// metrics
//	public static final String IPF = "ipf";
//	public static final String HIST_LEN = "hist_len";
//	public static final String IDF = "idf";
//	public static final String TF = "tf";
//	public static final String LENGTH = "length";
//	public static final String POSITION = "position";
//	public static final String NER_BELIEF = "belief(ner)";
//	public static final String SER_BELIEF = "belief(ser)";


	private final TreeSet<Token> token = new TreeSet<Token>();
	
	private final T value;

	public TokenSequence(T value) {
		this.value = value;
	}
	
	public void addToken(Token t) {
		token.add(t);
	}
	
	public int getStart() {
		return token.first().getStart();
	}
	
	public int getEnd() {
		return token.last().getEnd();
	}
	
//
//	public double getBelief() {
//
//		if(super.getDataSheet() == null) return 0;
//		
//		Double belief = null;
//		if (get(IS_STRUCTURED_ENTITY) != null) {
//			belief = get(SER_BELIEF);
//		} else {
//			belief = get(NER_BELIEF);
//		}
//		if (belief == null)
//			belief = 0.0;
//		return belief;
//	}

	public List<Token> getTokens() {
		return new ArrayList<Token>(token);
	}
	
	public String[] toArray() {
		
		String[] a = new String[token.size()];
		int i = 0;
		for(Token t :token) {
			a[i++] = t.toString();
		}
		
		return a;
	}

	public T getValue() {
		return value;
	}

	@Override
	public String toString() {
		return token.first().getTextSource().substring(token.first().getStart(), token.last().getEnd());
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(TokenSequence<T> o) {
		return this.token.first().compareTo(o.token.first());
	}

}

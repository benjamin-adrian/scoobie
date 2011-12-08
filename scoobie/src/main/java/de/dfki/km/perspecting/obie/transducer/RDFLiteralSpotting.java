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

package de.dfki.km.perspecting.obie.transducer;

import gnu.trove.TIntHashSet;

import java.io.BufferedReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

import de.dfki.km.perspecting.obie.connection.KnowledgeBase;
import de.dfki.km.perspecting.obie.connection.RemoteCursor;
import de.dfki.km.perspecting.obie.model.Document;
import de.dfki.km.perspecting.obie.model.SemanticEntity;
import de.dfki.km.perspecting.obie.model.TextPointer;
import de.dfki.km.perspecting.obie.model.Token;
import de.dfki.km.perspecting.obie.model.TokenSequence;
import de.dfki.km.perspecting.obie.transducer.model.SuffixArray;
import de.dfki.km.perspecting.obie.workflow.Transducer;

public class RDFLiteralSpotting extends Transducer {

	private final Logger log = Logger.getLogger(RDFLiteralSpotting.class
			.getName());

	private boolean filterLongestMatches = true;

	public void setFilterLongestMatch(boolean flag) {
		this.filterLongestMatches = flag;
	}

	@Override
	public void transduce(Document document, KnowledgeBase kb) throws Exception {
		SuffixArray suffixes = document.getSuffixArray();
		TIntHashSet datatypePropertyFilter = document.getFilterContext()
				.getDatatypePropertyIndexFilter();

		if (suffixes.getCommonPrefixStrings().length > 0) {

			log.info("Request Symbols Candidates");

			long start = System.currentTimeMillis();
			final RemoteCursor values = kb.getDatatypePropertyValues(
					datatypePropertyFilter.toArray(), suffixes
							.getCommonPrefixStrings());
			log.info("Request Symbols Candidates took: "
					+ (System.currentTimeMillis() - start));
			start = System.currentTimeMillis();
			log.info("Starting SuffixArrayComparison");
			final List<TextPointer> symbols = suffixes.compare(values);
			log.info("SuffixArrayComparison took: "
					+ (System.currentTimeMillis() - start));

			values.close();
			Collections.sort(symbols);

			List<TextPointer> cleanedsymbols;

			if (filterLongestMatches) {
				cleanedsymbols = filterLongestMatches(kb, symbols);
			} else {
				cleanedsymbols = symbols;
			}

			cleanedsymbols = filterCaseMatches(cleanedsymbols);

			log.info("Found " + cleanedsymbols.size() + " matches.");

			final ArrayList<Token> tokenList = new ArrayList<Token>();
			for (TextPointer literal : cleanedsymbols) {
				tokenList.clear();
				final Iterator<Token> iter = document.getTokens().iterator();
				Token segment = iter.next();

				while (iter.hasNext() && segment.getStart() < literal.getA()) {
					segment = iter.next();
				}

				while (segment.getStart() < literal.getB()) {
					if (segment.getEnd() <= literal.getB()) {
						tokenList.add(segment);
					}

					if (iter.hasNext()) {
						segment = iter.next();
					} else {
						break;
					}
				}
				// check correct starting and ending
				if (!tokenList.isEmpty()
						&& tokenList.get(0).getStart() == literal.getA()
						&& tokenList.get(tokenList.size() - 1).getEnd() == literal
								.getB()) {

					for (int i = 0; i < tokenList.size(); i++) {
						if (i == 0) {
							tokenList.get(i).addProperty("B",
									literal.getLiteralValueIndex(),
									literal.getDatatypePropertyIndex());
						} else {
							tokenList.get(i).addProperty("I",
									literal.getLiteralValueIndex(),
									literal.getDatatypePropertyIndex());
						}
					}
				}
			}

		}

	}

	/**
	 * Input data in line based form:
	 * 
	 * property \t literal \n
	 * 
	 * @return
	 */
	public String compare(Document document, KnowledgeBase kb, Reader gt)
			throws Exception {

		HashSet<String> relevantLines = new HashSet<String>();

		BufferedReader br = new BufferedReader(gt);
		for (String line = br.readLine(); line != null; line = br.readLine()) {
			relevantLines.add(line);
		}

		HashSet<String> foundLines = new HashSet<String>();

		for (TokenSequence<SemanticEntity> ts : document
				.getRetrievedPropertyValues()) {
			SemanticEntity se = ts.getValue();
			String uri = kb.getURI(se.getPropertyIndex());
			String value = ts.toString();

			foundLines.add(uri + "\t" + value);
		}

		HashSet<String> foundRelevantLines = new HashSet<String>(relevantLines);
		foundRelevantLines.retainAll(foundLines);

		double recall = ((double) foundRelevantLines.size())
				/ relevantLines.size();
		double precision = ((double) foundRelevantLines.size())
				/ foundLines.size();

		return String.format("%i\t%i\t%d\t%d\n", relevantLines.size(),
				foundLines.size(), recall, precision);
	}

	/**
	 * Just return longest phrases
	 * 
	 * @param ontology
	 * @param symbols
	 * @return
	 * @throws Exception
	 */
	private List<TextPointer> filterLongestMatches(KnowledgeBase kb,
			final List<TextPointer> symbols) throws Exception {
		final List<TextPointer> cleanedsymbols = new ArrayList<TextPointer>();
		TextPointer s0 = null;

		for (int i = 0; i < symbols.size(); i++) {
			TextPointer s = symbols.get(i);
			if (s0 == null) {
				s0 = s;
				cleanedsymbols.add(s);
			} else {
				if (s0.length() > s.length() && s.getA() >= s0.getA()
						&& s.getB() <= s0.getB()) {
					// log(ontology.getSession(), record.getDocument().getUri(),

					// System.out.println("Removing partial symbol of: \"" + s0
					// + "\" ie., " + s);
					// System.out.println(kb.getLiteral(s0.getLiteralValueIndex()));

					// "Removing partial symbol of " + s0 + ": " + s,
					// Level.INFO);
				} else {
					s0 = s;
					cleanedsymbols.add(s);
				}
			}
		}
		return cleanedsymbols;
	}

	private List<TextPointer> filterCaseMatches(final List<TextPointer> symbols)
			throws Exception {
		final List<TextPointer> cleanedsymbols = new ArrayList<TextPointer>();

		HashSet<String> correctMatchingCase = new HashSet<String>();

		for (TextPointer s : symbols) {
			String textValue = s.toString();
			String literalValue = s.getLiteral();
			if (textValue.equals(literalValue)) {
				correctMatchingCase.add(textValue);
			}
		}

		for (TextPointer s : symbols) {
			String textValue = s.toString();
			String literalValue = s.getLiteral();
			if (correctMatchingCase.contains(textValue)) {
				if (textValue.equals(literalValue)) {
					cleanedsymbols.add(s);
				} else {
					// System.out.println("Removed: " + literalValue +
					// " for text: " + textValue);
				}
			} else {
				cleanedsymbols.add(s);
			}
		}

		return cleanedsymbols;
	}
}

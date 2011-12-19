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

import java.util.ArrayList;
import java.util.Collections;
import java.util.logging.Logger;

import de.dfki.km.perspecting.obie.connection.KnowledgeBase;
import de.dfki.km.perspecting.obie.model.Document;
import de.dfki.km.perspecting.obie.model.Token;
import de.dfki.km.perspecting.obie.model.TokenSequence;
import de.dfki.km.perspecting.obie.transducer.model.LiteralHashing;
import de.dfki.km.perspecting.obie.transducer.model.SuffixArray;
import de.dfki.km.perspecting.obie.workflow.Transducer;

public class SuffixArrayBuilder extends Transducer {

	private final Logger log = Logger.getLogger(SuffixArrayBuilder.class
			.getName());

	private int maxSuffixLength;

	private boolean filterNounPhrases = true;

	private LiteralHashing hashing;

	public void filterNounPhrases(boolean filterNounPhrases) {
		this.filterNounPhrases = filterNounPhrases;
	}

	public SuffixArrayBuilder(int maxSuffixLength, LiteralHashing hashing) {
		this.maxSuffixLength = maxSuffixLength;
		this.hashing = hashing;
		log.info("Set maxSuffixLength to " + maxSuffixLength);
	}

	@Override
	public void transduce(Document document, KnowledgeBase kb) throws Exception {

		final ArrayList<Token> phrasedTokens = new ArrayList<Token>();

		if (filterNounPhrases) {
			for (TokenSequence<String> np : document.getNounPhrases()) {
				for (Token t : np.getTokens()) {
					phrasedTokens.add(t);
				}
			}
		} else {
			for (Token t : document.getTokens()) {
				phrasedTokens.add(t);
			}
		}

		Collections.sort(phrasedTokens);

		long start = System.currentTimeMillis();
		document.setSuffixArray(new SuffixArray(phrasedTokens, kb, hashing,
				maxSuffixLength));
		log.info("Time to build suffix array: "
				+ (System.currentTimeMillis() - start));

	}

}

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

import java.util.List;
import java.util.logging.Logger;

import opennlp.tools.postag.POSTagger;
import de.dfki.km.perspecting.obie.connection.KnowledgeBase;
import de.dfki.km.perspecting.obie.model.Document;
import de.dfki.km.perspecting.obie.model.Token;
import de.dfki.km.perspecting.obie.model.TokenSequence;
import de.dfki.km.perspecting.obie.workflow.Transducer;

public class POSTagging extends Transducer {

	private final Logger log = Logger.getLogger(POSTagging.class.getName());

	private POSTagger detector;

	public POSTagging(POSTagger posTagger) {
		this.detector = posTagger;
	}

	@Override
	public void transduce(Document document, KnowledgeBase kb) throws Exception {

		log.info("Performing POS Tagging in " + document.getLanguage());
		for (TokenSequence<Integer> sentence : document.getSentences()) {
			List<Token> tokenList = sentence.getTokens();
			String[] tokens = sentence.toArray();
			String[] posTags = detector.tag(tokens);

			for (int i = 0; i < posTags.length; i++) {
				tokenList.get(i).setPOS(posTags[i]);
			}
		}

	}

}
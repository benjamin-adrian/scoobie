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

import java.util.logging.Level;
import java.util.logging.Logger;

import de.dfki.km.perspecting.obie.connection.KnowledgeBase;
import de.dfki.km.perspecting.obie.model.Document;
import de.dfki.km.perspecting.obie.model.TokenSequence;
import de.dfki.km.perspecting.obie.transducer.model.CRFNounPhraseChunkerModel;
import de.dfki.km.perspecting.obie.workflow.Transducer;


public class ProperNameRecognition extends Transducer {

	private final Logger log = Logger.getLogger(ProperNameRecognition.class
			.getName());
	private CRFNounPhraseChunkerModel chunker;
	
	public ProperNameRecognition(CRFNounPhraseChunkerModel chunker) {
		this.chunker = chunker;
	}
	
	@Override
	public void transduce(Document document, KnowledgeBase kb)
			throws Exception {
		try {
			
			for (TokenSequence<Integer> sentence : document.getSentences()) {
				chunker.test(sentence);
			}

		} catch (Exception e) {
			log.log(Level.WARNING, e.getMessage(), e);
		}
	}

}

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

import java.text.BreakIterator;
import java.util.List;
import java.util.Locale;
import java.util.logging.Logger;

import de.dfki.km.perspecting.obie.connection.KnowledgeBase;
import de.dfki.km.perspecting.obie.model.Document;
import de.dfki.km.perspecting.obie.model.Token;
import de.dfki.km.perspecting.obie.workflow.Transducer;

public class SentenceSegmenter extends Transducer {

	private final Logger log = Logger.getLogger(SentenceSegmenter.class
			.getName());

	@Override
	public void transduce(Document document, KnowledgeBase kb)
			throws Exception {
		final String text = document.getPlainTextContent();
		Locale l = new Locale(document.getLanguage().getValue());
		BreakIterator boundary = BreakIterator.getSentenceInstance(l);
		boundary.setText(text);

		final List<Token> tokens = document.getTokens();
		int sid = 0;
		int cToken = 0;
		for (int end = boundary.next(); end != BreakIterator.DONE; end = boundary
				.next()) {
			while (cToken < tokens.size() && tokens.get(cToken).getEnd() <= end) {
				tokens.get(cToken).setSentence(sid);
				cToken++;
			}
			sid++;
		}

		log.info("Found " + (sid + 1) + " sentences.");
	}
}

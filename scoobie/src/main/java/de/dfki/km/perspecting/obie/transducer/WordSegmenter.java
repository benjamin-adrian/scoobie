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
import java.util.ArrayList;
import java.util.Locale;
import java.util.logging.Logger;

import de.dfki.km.perspecting.obie.connection.KnowledgeBase;
import de.dfki.km.perspecting.obie.model.Document;
import de.dfki.km.perspecting.obie.workflow.Transducer;

public class WordSegmenter extends Transducer {

	private final Logger log = Logger.getLogger(WordSegmenter.class
			.getName());

	public void transduce(Document document, KnowledgeBase kb)
			throws Exception {
		String text = document.getPlainTextContent();
		Locale l = new Locale(document.getLanguage().getValue());
		BreakIterator boundary = BreakIterator.getWordInstance(l);
		boundary.setText(text);
		int start = boundary.first();
		int count = 0;

		for (int end = boundary.next(); end != BreakIterator.DONE; start = end, end = boundary
				.next()) {
			if (!Character.isWhitespace(text.charAt(start))) {
				document.getData().createToken(start, end);
				count++;
			}
		}
		log.info("Found: " + count+ " tokens in text");

	}

	public String[] segment(String phrase) {
		BreakIterator boundary = BreakIterator.getWordInstance();
		boundary.setText(phrase);
		int start = boundary.first();
		ArrayList<String> comps = new ArrayList<String>();
		for (int end = boundary.next(); end != BreakIterator.DONE; start = end, end = boundary
				.next()) {
			if (!Character.isWhitespace(phrase.charAt(start))) {
				comps.add(phrase.substring(start, end));
			}
		}
		return comps.toArray(new String[comps.size()]);
	}


}

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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import de.dfki.km.perspecting.obie.connection.KnowledgeBase;
import de.dfki.km.perspecting.obie.model.Document;
import de.dfki.km.perspecting.obie.model.Token;
import de.dfki.km.perspecting.obie.workflow.Transducer;

public class RegularStructuredEntityRecognition extends Transducer {

	private String[] patterns;
	
	public RegularStructuredEntityRecognition(String[] patterns) {
		this.patterns = patterns;
	}

	@Override
	public void transduce(final Document document, final KnowledgeBase kb)
			throws Exception {
		
		for(String regex : patterns) {
			transduce(document, regex);
		}
	}
	
	private void transduce(Document document, String regex) {

		Pattern pattern = Pattern.compile(regex);
		String text = document.getPlainTextContent();
		Matcher matcher = pattern.matcher(text);
		while (matcher.find()) {
			int start = matcher.start();
			int end = matcher.end();
			List<Token> token = document.getTokens(start, end);

			for (int i = 0; i < token.size(); i++) {

				String position = "I";

				if (i == 0) {
					position = "B";
				}
				token.get(i).addRegexMatch(position, regex);
			}
		}
	}

}

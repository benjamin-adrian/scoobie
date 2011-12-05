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

package de.dfki.km.perspecting.obie.transducer.model;

import java.io.InputStream;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.List;
import java.util.logging.Logger;

import de.dfki.km.perspecting.obie.model.Token;
import de.dfki.km.perspecting.obie.model.TokenSequence;

public class CRFNounPhraseChunkerModel {

	private static final String UTF_8 = "UTF-8";

	private static final String NEWLINE = "\n";

	private static final String SPACE = " ";


	private final Logger log = Logger.getLogger(CRFNounPhraseChunkerModel.class
			.getName());

	private String path;

	public CRFNounPhraseChunkerModel(String modelPath) {
		this.path = modelPath;
	}
	
	public CRFNounPhraseChunkerModel(InputStream input) throws Exception {
		npc = new NounPhraseChunker(input);
	}

	private StringReader getTestInstance(TokenSequence<Integer> posTags)
			throws UnsupportedEncodingException {

		StringBuilder builder = new StringBuilder();

		for (Token s1 : posTags.getTokens()) {
			String s = URLEncoder.encode(s1.toString(), UTF_8);
			builder.append(s.toString() + SPACE + s1.getPartOfSpeechTag() + NEWLINE);
		}

		return new StringReader(builder.toString());
	}

	private NounPhraseChunker npc = null;

	public void test(TokenSequence<Integer> sentence)
			throws Exception {

		StringReader testDataString = getTestInstance(sentence);

		if (npc == null) {
			npc = new NounPhraseChunker(path);
		}

		List<String> list = npc.test(testDataString);
		testDataString.close();

		for (int i = 0; i < list.size(); i++) {
			String v = list.get(i);
			sentence.getTokens().get(i).setNounPhraseTag(v);
		}
	}

}

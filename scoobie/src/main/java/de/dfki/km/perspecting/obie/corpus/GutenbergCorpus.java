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

package de.dfki.km.perspecting.obie.corpus;

import java.io.BufferedReader;
import java.io.File;
import java.io.Reader;
import java.io.StringReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import de.dfki.km.perspecting.obie.vocabulary.Language;
import de.dfki.km.perspecting.obie.vocabulary.MediaType;

public class GutenbergCorpus extends LabeledTextCorpus {

	private final static Pattern p = Pattern.compile(
			"(\"http://dbpedia.org/\\w+/\\w+\")", Pattern.CASE_INSENSITIVE);

	public GutenbergCorpus(File labelFolder, TextCorpus corpus)
			throws Exception {
		super(labelFolder, MediaType.ZIP, corpus);
	}

	public GutenbergCorpus() throws Exception {
		this(new File("../corpora/gutenberg/gutenberg_text_labels.zip"),
				new TextCorpus(new File(
						"../corpora/gutenberg/gutenberg_text_labels.zip"),
						MediaType.ZIP, MediaType.HTML, Language.EN));
	}

	@Override
	public Reader extractLabels(Reader in) throws Exception {

		BufferedReader br = new BufferedReader(in);

		final StringBuffer b = new StringBuffer();

		for (String line = br.readLine(); line != null; line = br.readLine()) {
			Matcher m = p.matcher(line);
			while (m.find()) {
				b.append(m.group().replaceAll("/page/", "/resource/")
						.replaceAll("\"", ""));
				b.append("\n");
			}
		}
		return new StringReader(b.toString());
	}

}

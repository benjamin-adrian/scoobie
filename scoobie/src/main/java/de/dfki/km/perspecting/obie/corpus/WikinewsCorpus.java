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

import java.io.File;
import java.io.Reader;
import java.io.StringReader;
import java.net.URI;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;

import de.dfki.km.perspecting.obie.model.DocumentProcedure;
import de.dfki.km.perspecting.obie.vocabulary.Language;
import de.dfki.km.perspecting.obie.vocabulary.MediaType;

public class WikinewsCorpus extends LabeledTextCorpus {
	
	private final static Pattern p = Pattern.compile("(\"http://dbpedia.org/\\w+/\\w+\")", Pattern.CASE_INSENSITIVE);

	public WikinewsCorpus(File folder) {
		super(folder, MediaType.HTML, Language.EN);
	}
	
	@SuppressWarnings("unchecked")
	public List<?> forEach(DocumentProcedure<?> p) throws Exception {
		List l = new ArrayList();
		for (File f : getFiles(corpus)) {
			log.info("processing file: " + f.getName());
			String content = FileUtils.readFileToString(f, "utf-8");
			l.add(p.process(new StringReader(content), f.toURI()));
		}
		return l;
	}
	
	@Override
	public Reader getGroundTruth(URI uri) throws Exception {
			
		String content = FileUtils.readFileToString(new File (URLDecoder.decode(uri.toString(), "utf-8").replaceAll("file:", "")), "utf-8");
		Matcher m = p.matcher(content);

		final StringBuffer b = new StringBuffer();
		
		while (m.find()) {
			b.append(m.group().replaceAll("/page/", "/resource/").replaceAll("\"", ""));
			b.append("\n");
		}
		
		return new StringReader(b.toString());
		
	}

}

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
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.Reader;
import java.io.StringReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import de.dfki.km.perspecting.obie.model.DocumentProcedure;
import de.dfki.km.perspecting.obie.vocabulary.Language;
import de.dfki.km.perspecting.obie.vocabulary.MediaType;

public class ConllCorpus extends TextCorpus {

	private static final String NEWLINE = "\n";
	private static final String SPACE = " ";
	
	private static final String DOCSTART_X_O_O = "-DOCSTART-";

	public ConllCorpus(File corpusDir) throws Exception {
		super(corpusDir, MediaType.ZIP, MediaType.TEXT, Language.EN);
	}
	
	
	public TextCorpus createPlainTextCorpus(final File outputFolder) throws Exception {
		outputFolder.mkdirs();

		this.forEach(new DocumentProcedure<File>() {
			@Override
			public File process(Reader reader, URI uri) throws Exception {

				BufferedReader br = new BufferedReader(reader);

				File file = new File(outputFolder+"/"+uri);
				BufferedWriter writer = new BufferedWriter(new FileWriter(file));
				
				for (String line = br.readLine(); line != null; line = br
						.readLine()) {
					if (line.isEmpty()) {
						writer.append(NEWLINE);
					} else {
						String[] lineSplit = line.split(SPACE);
						writer.append(lineSplit[0]);
						writer.append(SPACE);
					}
				}
				writer.close();
				return file;
			}

		});
		
		return new TextCorpus(outputFolder, MediaType.DIRECTORY, MediaType.TEXT, Language.EN);
	}

	@SuppressWarnings("unchecked")
	@Override
	public List<?> forEach(DocumentProcedure<?> p) throws Exception {

		BufferedReader reader = new BufferedReader(new FileReader(corpus));
		StringBuffer buffer = new StringBuffer();

		List list = new ArrayList();

		int count = 0;

		for (String line = reader.readLine(); line != null; line = reader
				.readLine()) {
			if (line.startsWith(DOCSTART_X_O_O)) {
				count++;
				if (buffer.length() > 0) {
					list.add(p.process(new StringReader(buffer.toString()), new URI(this.corpus.toURI()+"#"+ count)));
				}
				buffer.setLength(0);
			} else {
				buffer.append(line);
				buffer.append(NEWLINE);
			}
		}
		reader.close();

		return list;
	}
}

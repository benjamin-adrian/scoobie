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

import org.openrdf.model.Statement;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.rio.RDFFormat;
import org.openrdf.sail.memory.MemoryStore;

import de.dfki.km.perspecting.obie.vocabulary.Language;
import de.dfki.km.perspecting.obie.vocabulary.MediaType;

public class BBCMusicCorpus extends LabeledTextCorpus {

	private String labels;

	final org.openrdf.model.URI foafname = new URIImpl(
			"http://xmlns.com/foaf/0.1/name");

	public BBCMusicCorpus(File folder, String labels) {
		super(folder, MediaType.HTML, Language.EN);
		this.labels = labels;
	}

	@Override
	public Reader getGroundTruth(URI uri) throws Exception {
		File labelFile = new File(labels
				+ new File(URLDecoder.decode(uri.toURL().getFile(), "utf-8"))
						.getName() + ".rdf");

		SailRepository sr = new SailRepository(new MemoryStore());
		SailRepositoryConnection conn;
		sr.initialize();
		conn = sr.getConnection();
		conn.add(labelFile, "http://www.bbc.co.uk/", RDFFormat.RDFXML);

		final StringBuffer b = new StringBuffer();

		for (Statement stmt : conn.getStatements(null, foafname, null, true)
				.asList()) {
			b.append(stmt.getSubject().stringValue());
			b.append("\n");
		}
		return new StringReader(b.toString());
	}

}

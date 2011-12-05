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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.net.URI;
import java.net.URLDecoder;
import java.util.Collection;

import org.apache.commons.io.FileUtils;
import org.junit.Test;
import org.openrdf.model.Statement;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.ntriples.NTriplesWriter;
import org.openrdf.sail.memory.MemoryStore;


public class BBCNatureCorpus extends LabeledTextCorpus {

	public BBCNatureCorpus(File labelFolder, TextCorpus corpus) {
		super(labelFolder, corpus);
	}

	@Override
	public Reader getGroundTruth(URI uri) throws Exception {
		File labelFile = new File(super.getCorpus().getAbsoluteFile()
				+ "/"
				+ URLDecoder.decode(new File(uri.toURL().getFile()).getName(),
						"utf-8"));

		SailRepository sr = new SailRepository(new MemoryStore());
		SailRepositoryConnection conn;
		sr.initialize();
		conn = sr.getConnection();
		conn.add(labelFile, "http://www.bbc.co.uk/", RDFFormat.RDFXML);

		final StringBuffer b = new StringBuffer();

		for (Statement stmt : conn.getStatements(null, RDFS.LABEL, null, true)
				.asList()) {
			b.append(stmt.getSubject().stringValue());
			b.append("\n");
		}
		for (Statement stmt : conn.getStatements(null,
				new URIImpl("http://purl.org/dc/terms/title"), null, true)
				.asList()) {
			b.append(stmt.getSubject().stringValue());
			b.append("\n");
		}
		return new StringReader(b.toString());
	}

	@Test
	public void run() throws Exception,
			IOException {

		String rdf = "/media/Daten/corpora/wildlife/rdf/";

		File folder = new File(rdf);
		
		
		SailRepository sr = new SailRepository(new MemoryStore());
		SailRepositoryConnection conn;
		sr.initialize();
		conn = sr.getConnection();
		
		
		
		for (File f : (Collection<File>) FileUtils.listFiles(folder,
				new String[] { "rdf" }, false)) {
			
			conn.add(f, "http://www.bbc.co.uk/", RDFFormat.RDFXML);
			
//			String name = f.getName();
//			String url = URLDecoder.decode(name, "utf-8").replaceAll("\\.rdf",
//					"");
//
//			File newfile = new File("/media/Daten/corpora/wildlife/text/"
//					+ name);
//			if (!newfile.exists())
//				try {
//					FileUtils.copyURLToFile(new URL(url), newfile);
//				} catch (Exception e) {
//					e.printStackTrace();
//				}

		}
		
		conn.export(new NTriplesWriter(new FileOutputStream("/media/Daten/corpora/wildlife/wildlife3.nt")));

	}

}

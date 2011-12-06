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
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.net.URI;
import java.net.URLDecoder;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.apache.commons.io.FileUtils;
import org.openrdf.model.Statement;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.rio.RDFFormat;
import org.openrdf.sail.memory.MemoryStore;

import de.dfki.km.perspecting.obie.vocabulary.MediaType;

public class BBCMusicCorpus extends LabeledTextCorpus {

	final org.openrdf.model.URI foafname = new URIImpl(
			"http://xmlns.com/foaf/0.1/name");
		
	public BBCMusicCorpus(File labelFolder, TextCorpus corpus) throws Exception {
		super(labelFolder, MediaType.ZIP, corpus);
	}


	public Reader getGroundTruth(final URI uri) throws Exception {
		if (labelFileMediaType == MediaType.DIRECTORY) {
			return new StringReader(FileUtils.readFileToString(new File(uri)));
		} else if (labelFileMediaType == MediaType.ZIP) {
			ZipFile zipFile = new ZipFile(labelFolder);
			String[] entryName = uri.toURL().getFile().split("/");
			ZipEntry entry = zipFile.getEntry(URLDecoder.decode(
					entryName[entryName.length - 1], "utf-8").replace("txt", "dumps")+".rdf");

			if (entry != null) {
				log.info("found labels for: " + uri.toString());
			} else {
				throw new Exception("did not found labels for: " + uri.toString());
			}
			return new InputStreamReader(zipFile.getInputStream(entry));
		} else {
			throw new Exception("Unsupported media format for labels: "
					+ labelFileMediaType + ". "
					+ "Please use zip or plain directories instead.");
		}
	}

	/**
	 * This method is a hook for inserting a label extraction from different
	 * label files.
	 */
	protected Reader extractLabels(Reader in) throws Exception {
		
		SailRepository sr = new SailRepository(new MemoryStore());
		SailRepositoryConnection conn;
		sr.initialize();
		conn = sr.getConnection();
		conn.add(in, "http://www.bbc.co.uk/", RDFFormat.RDFXML);

		final StringBuffer b = new StringBuffer();

		for (Statement stmt : conn.getStatements(null, foafname, null, true)
				.asList()) {
			b.append(stmt.getSubject().stringValue());
			b.append("\n");
		}
		return new StringReader(b.toString());
		
	}
	

}

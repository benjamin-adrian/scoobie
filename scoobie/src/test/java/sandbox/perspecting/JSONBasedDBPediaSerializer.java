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

package sandbox.perspecting;

import java.io.StringReader;
import java.net.URL;
import java.util.List;

import org.json.simple.JSONObject;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.rio.RDFFormat;
import org.openrdf.sail.memory.MemoryStore;

import de.dfki.km.perspecting.obie.connection.KnowledgeBase;
import de.dfki.km.perspecting.obie.model.Document;
import de.dfki.km.perspecting.obie.model.SemanticEntity;
import de.dfki.km.perspecting.obie.model.TokenSequence;
import de.dfki.km.perspecting.obie.postprocessor.Serializer;

/*
 * plainUI.data.relations = {
 "Rainer Brüderle": {
 "Born": "22 June 1945",
 "Born in": "Berlin",
 "Nationality": "Germany",
 "Political party": "FDP",
 "Alma mater": " University of Mainz",
 "Profession": "Economist",
 "Website": "www.rainer-bruederle.de",
 "Political role": "Federal Minister for Economics and Technology",
 "_img", "http://server.com/image.jpg",
 },
 "Federal Ministry of Economics and Technology (Germany)": {
 "Formed": "1917 (as the Reichswirtschaftsamt)",
 "Jurisdiction": "Government of Germany",
 "Headquarters": "Berlin",
 "Minister responsible": "Rainer Brüderle",
 "Website": "www.bmwi.de"
 },
 "Internet of Services": { "Website": "www.internet-of-services.com" }
 }

 */

public class JSONBasedDBPediaSerializer implements Serializer {

	private String dbpediaMirror = "pc-4323:8890";

	public void setDbpediaMirror(String dbpediaMirror) {
		this.dbpediaMirror = dbpediaMirror;
	}
	
	public String getDbpediaMirror() {
		return dbpediaMirror;
	}
	
	@Override
	public StringReader serialize(Document document, KnowledgeBase kb) throws Exception {

		List<TokenSequence<SemanticEntity>> results = document
				.getResolvedSubjects();

		JSONObject json = new JSONObject();

		for (TokenSequence<SemanticEntity> result : results) {
			String uri = result.getValue().getSubjectURI();
			String text = document.getPlainTextContent().substring(
					result.getStart(), result.getEnd());

			if (!text.matches("[0-9]+")) {
				JSONObject entry = getLinkedDataInfo(uri);
				// if (entry.containsKey("_abstract")) {
				json.put(text, entry);
				// }
			}
		}

		return new StringReader(json.toJSONString());
	}

	@SuppressWarnings("unchecked")
	private JSONObject getLinkedDataInfo(String uri) throws Exception {

		
		
		
		
		SailRepository sr = new SailRepository(new MemoryStore());
		SailRepositoryConnection conn;
		sr.initialize();
		conn = sr.getConnection();

		JSONObject json = new JSONObject();

		System.out.println("Lookup: " + uri);

		// hack to get data from local DBpedia mirror

		String urlString = uri.replaceFirst("http://dbpedia.org/resource/",
				"http://"+dbpediaMirror+"/data/");

		URL url = new URL(urlString);

		// only absolute URIs are assumed
		conn.add(url, "", RDFFormat.RDFXML);

		for (Statement stmt : conn.getStatements(null,
				new URIImpl("http://"+dbpediaMirror+"/ontology/wikiPageRedirects"),
				null, false).asList()) {
			sr = new SailRepository(new MemoryStore());
			sr.initialize();
			conn = sr.getConnection();

			uri = stmt.getObject().stringValue().replace("resource", "data");

			System.out.println("Found redirect from " + url + " to "
					+ stmt.getObject().stringValue());
			conn.add(new URL(uri), "", RDFFormat.RDFXML);
		}

		System.out.println("found " + conn.size() + " triples.");
		json.put("_uri", uri);
		json.put("group", uri.hashCode());

		for (Statement stmt : conn.getStatements(
				new URIImpl(uri.replaceFirst("data", "resource")), null, null,
				false).asList()) {

			String predicate = stmt.getPredicate().toString().substring(
					stmt.getPredicate().toString().lastIndexOf("/") + 1);

			predicate = predicate.substring(predicate.indexOf("#") + 1);

			String predicate2 = "";

			for (char c : predicate.toCharArray()) {
				if (Character.isUpperCase(c)) {
					predicate2 += " ";
				}
				predicate2 += Character.toLowerCase(c);
			}

			predicate = predicate2;

			if (stmt.getObject() instanceof Literal) {
				String object = stmt.getObject().stringValue();
				if (!object.contains("px") && !object.contains("{")) {

					if (predicate.equals("abstract")
							|| predicate.equals("comment")) {
						int i = stmt.getObject().stringValue().indexOf(". ");
						if (i > 0) {
							json.put("_abstract", stmt.getObject()
									.stringValue().substring(0, i));
						} else {
							json.put("_abstract", stmt.getObject()
									.stringValue());
						}
					} else if (predicate.contains(" ")) {
						json.put(predicate, stmt.getObject().stringValue());
					}
				}
			} else if (predicate.equals("depiction")) {
				json.put("_img", stmt.getObject().stringValue());
			} else if (predicate.equals("page")) {
				json.put("_wikipedia", stmt.getObject().stringValue());
			} else if (!predicate.endsWith("same as")
					&& !predicate.endsWith("thumbnail")) {

				String object = stmt.getObject().stringValue().substring(
						stmt.getObject().stringValue().lastIndexOf("/") + 1);
				if (!object.contains(":") && !predicate.startsWith("wiki")) {
					json.put(predicate, object.replaceAll("_", " "));
				}
			}

		}

		return json;
	}

}

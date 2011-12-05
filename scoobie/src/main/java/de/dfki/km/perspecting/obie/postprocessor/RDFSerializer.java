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

package de.dfki.km.perspecting.obie.postprocessor;

import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;

import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.rio.trig.TriGWriter;
import org.openrdf.sail.memory.MemoryStore;

import de.dfki.km.perspecting.obie.connection.KnowledgeBase;
import de.dfki.km.perspecting.obie.model.Document;
import de.dfki.km.perspecting.obie.model.RDFEdge;
import de.dfki.km.perspecting.obie.model.SemanticEntity;
import de.dfki.km.perspecting.obie.model.TokenSequence;

public class RDFSerializer implements Serializer {

	
	private String namespace;
	private URIImpl entityGraph;
	private URIImpl predictionGraph;
	private URIImpl knownGraph;
	
	public RDFSerializer(String namespace) {
		this.namespace = namespace;
		this.entityGraph = new URIImpl(this.namespace + "#recognized");
		this.predictionGraph = new URIImpl(this.namespace + "#predicted");
		this.knownGraph = new URIImpl(this.namespace + "#known");
	}
	
	
	@Override
	public Reader serialize(Document document, KnowledgeBase kb) throws Exception {
		MemoryStore store = new MemoryStore();
		SailRepository repos = new SailRepository(store);
		repos.initialize();
		SailRepositoryConnection conn = repos.getConnection();
		
		for(TokenSequence<SemanticEntity> ts : document.getResolvedSubjects()) {
			
			String subject = ts.getValue().getSubjectURI();
			String predicate = kb.getURI(ts.getValue().getPropertyIndex());
			String object = ts.toString();
			
			conn.add(new StatementImpl(new URIImpl(subject), new URIImpl(predicate), new LiteralImpl(object, "en")), entityGraph);

			for(int type : ts.getTokens().get(0).getTypes(1.0).toArray()) {
				conn.add(new StatementImpl(new URIImpl(subject), RDF.TYPE, new URIImpl(kb.getURI(type))), entityGraph);
			}

		}
		
		for(TokenSequence<SemanticEntity> ts : document.getResolvedSubjects()) {
			String subject = ts.getValue().getSubjectURI();
			for(int type : ts.getTokens().get(0).getPredictedTypes(0).toArray()) {
				conn.add(new StatementImpl(new URIImpl(subject), RDF.TYPE, new URIImpl(kb.getURI(type))), predictionGraph);
			}
		}
		
		for(int v : document.getPredictionGraph().getVertices()) {
			for(RDFEdge edge : document.getPredictionGraph().getOutEdges(v)) {
				int v1 = document.getPredictionGraph().getDest(edge);
				
				conn.add(new StatementImpl(new URIImpl(kb.getURI(v)), new URIImpl(kb.getURI(edge.getPredicate())), new URIImpl(kb.getURI(v1))), predictionGraph);
			}
		}
		
		
		for(int v : document.getGraph().getVertices()) {
			for(RDFEdge edge : document.getGraph().getOutEdges(v)) {
				int v1 = document.getGraph().getDest(edge);
				
				conn.add(new StatementImpl(new URIImpl(kb.getURI(v)), new URIImpl(kb.getURI(edge.getPredicate())), new URIImpl(kb.getURI(v1))), knownGraph);
			}
		}
		
		conn.commit();
		StringWriter w = new StringWriter();
		conn.export(new TriGWriter(w));
		conn.close();
		repos.shutDown();
		store.shutDown();
		return new StringReader(w.toString());
	}

}

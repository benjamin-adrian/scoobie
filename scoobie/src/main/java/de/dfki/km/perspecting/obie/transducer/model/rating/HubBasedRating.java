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

package de.dfki.km.perspecting.obie.transducer.model.rating;

import gnu.trove.TIntDoubleHashMap;
import gnu.trove.TIntHashSet;

import java.util.List;

import de.dfki.km.perspecting.obie.model.Document;
import de.dfki.km.perspecting.obie.model.RDFEdge;
import de.dfki.km.perspecting.obie.model.SemanticEntity;
import de.dfki.km.perspecting.obie.model.TokenSequence;
import edu.uci.ics.jung.algorithms.scoring.HITS;
import edu.uci.ics.jung.graph.DirectedGraph;

public class HubBasedRating implements RatingMetric {

	@Override
	public TIntDoubleHashMap getRating(Document document,
			List<TokenSequence<SemanticEntity>> entities) {

		DirectedGraph<Integer, RDFEdge> graph = document.getGraph();

		TIntHashSet nodes = new TIntHashSet();
		for (TokenSequence<SemanticEntity> ts : entities) {
			nodes.add(ts.getValue().getSubjectIndex());
		}

		TIntDoubleHashMap m = new TIntDoubleHashMap();
		HITS<Integer, RDFEdge> hits = new HITS<Integer, RDFEdge> (graph);
		hits.acceptDisconnectedGraph(true);
		hits.evaluate();
		
		for (int node : nodes.toArray()) {
			m.put(node, hits.getVertexScore(node).hub);

		}
		return m;
	}
	

}

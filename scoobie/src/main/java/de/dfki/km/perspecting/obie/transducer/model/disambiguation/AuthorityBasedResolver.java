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
package de.dfki.km.perspecting.obie.transducer.model.disambiguation;

import gnu.trove.TIntHashSet;
import gnu.trove.TIntObjectHashMap;

import java.util.Set;

import de.dfki.km.perspecting.obie.connection.KnowledgeBase;
import de.dfki.km.perspecting.obie.model.Document;
import de.dfki.km.perspecting.obie.model.RDFEdge;
import edu.uci.ics.jung.algorithms.scoring.HITS;
import edu.uci.ics.jung.graph.DirectedGraph;

public class AuthorityBasedResolver extends AmbiguityResolver {

	public String toString() {
		return "authority";
	};

	public TIntObjectHashMap<TIntHashSet> resolve(
			DirectedGraph<Integer, RDFEdge> graph,
			Set<Set<Integer>> literalSubjectPairs, Document document, KnowledgeBase kb) {

		TIntObjectHashMap<TIntHashSet> resolvedSubjects = new TIntObjectHashMap<TIntHashSet>();
		TIntHashSet ham = new TIntHashSet();
		TIntHashSet spam = new TIntHashSet();

		resolvedSubjects.put(0, spam);
		resolvedSubjects.put(1, ham);

		HITS<Integer, RDFEdge> hits = new HITS<Integer, RDFEdge>(graph);
		hits.acceptDisconnectedGraph(true);
		hits.evaluate();

		for (Set<Integer> c : literalSubjectPairs) {
			TIntHashSet subjects = getAmbiguousURIRefs(c);
			if (subjects.size() > 1) {
				double maxDegree = Double.NEGATIVE_INFINITY;
				for (int s : subjects.toArray()) {
					double authority = hits.getVertexScore(s).authority;

					if (authority > maxDegree) {
						maxDegree = authority;
						ham.clear();
						ham.add(s);
					} else if (authority == maxDegree) {
						ham.add(s);
					}
				}
				if (resolvedSubjects.size() < subjects.size()) {
					for (int s : subjects.toArray()) {
						if (!ham.contains(s)) {
							spam.add(s);
						}
					}
				}
			}
		}
		return resolvedSubjects;
	}

}

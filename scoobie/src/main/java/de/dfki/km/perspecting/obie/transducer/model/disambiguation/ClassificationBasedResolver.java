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
import de.dfki.km.perspecting.obie.model.SemanticEntity;
import de.dfki.km.perspecting.obie.model.TokenSequence;
import edu.uci.ics.jung.graph.DirectedGraph;

public class ClassificationBasedResolver extends AmbiguityResolver {


	public String toString() {
		return "classification";
	};


	public TIntObjectHashMap<TIntHashSet> resolve(DirectedGraph<Integer, RDFEdge> graph, Set<Set<Integer>> literalSubjectPairs, Document document, KnowledgeBase kb) {

		TIntObjectHashMap<TIntHashSet> resolvedSubjects = new TIntObjectHashMap<TIntHashSet>();

		TIntHashSet ham = new TIntHashSet();
		TIntHashSet spam = new TIntHashSet();

		resolvedSubjects.put(0, spam);
		resolvedSubjects.put(1, ham);

		TIntObjectHashMap<TIntHashSet> knownTypes = new TIntObjectHashMap<TIntHashSet>();
		TIntObjectHashMap<TIntHashSet> predictedTypes = new TIntObjectHashMap<TIntHashSet>();

		for (TokenSequence<SemanticEntity> ts : document.getResolvedSubjects()) {
			int subject = ts.getValue().getSubjectIndex();

			TIntHashSet knownTyping = knownTypes.get(subject);
			if (knownTyping == null) {
				knownTyping = new TIntHashSet();
				knownTypes.put(subject, knownTyping);
			}
			knownTyping.addAll(ts.getTokens().get(0).getTypes(1.0).toArray());

			TIntHashSet predictedTyping = predictedTypes.get(subject);
			if (predictedTyping == null) {
				predictedTyping = new TIntHashSet();
				predictedTypes.put(subject, predictedTyping);
			}
			predictedTyping.addAll(ts.getTokens().get(0).getPredictedTypes(0.0)
					.toArray());
		}

		try {
			for (Set<Integer> c : literalSubjectPairs) {
				TIntHashSet subjects = getAmbiguousURIRefs(c);

				if (subjects.size() > 1) {
					for (int s : subjects.toArray()) {
						TIntHashSet ktypes = knownTypes.get(s);
						if (ktypes != null && ktypes.size() > 0) {
							int ktype = kb.getCluster(ktypes.toArray());
							TIntHashSet ptypes = predictedTypes.get(s);
							if (ptypes.contains(ktype)) {
								ham.add(s);
							}
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
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resolvedSubjects;
	}

}

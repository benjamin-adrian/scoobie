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

package de.dfki.km.perspecting.obie.transducer;

import gnu.trove.TIntHashSet;

import java.util.logging.Logger;

import de.dfki.km.perspecting.obie.connection.KnowledgeBase;
import de.dfki.km.perspecting.obie.connection.ResultSetCursor;
import de.dfki.km.perspecting.obie.model.Document;
import de.dfki.km.perspecting.obie.model.RDFEdge;
import de.dfki.km.perspecting.obie.model.SemanticEntity;
import de.dfki.km.perspecting.obie.model.TokenSequence;
import de.dfki.km.perspecting.obie.workflow.Transducer;

/**
 * @author adrian
 * @version 0.1
 * @since 04.11.2009
 * 
 */
public class KnownFactsRetrieval extends Transducer {

	private Logger log = Logger.getLogger(KnownFactsRetrieval.class.getName());

	@Override
	public void transduce(Document document, KnowledgeBase kb)
			throws Exception {

		final TIntHashSet instances = new TIntHashSet();

//		final TIntHashSet unEntailedInstances = new TIntHashSet();

		for (TokenSequence<SemanticEntity> ts : document.getResolvedSubjects()) {
			int s = ts.getValue().getSubjectIndex();
			if (!instances.contains(s)) {
				instances.add(s);
			}
		}

//		for (int s : document.getGraph().getVertices()) {
//			if (instances.contains(s)) {
//				if (document.getGraph().getOutEdges(s).isEmpty()) {
//					unEntailedInstances.add(s);
//				}
//			}
//		}

		ResultSetCursor out = kb
				.getOutgoingRelations(instances.toArray());
		while (out.next()) {
			int s = out.getInt(1);
			int p = out.getInt(2);
			int o = out.getInt(3);
			
			document.getGraph().addEdge(new RDFEdge(p), s, o);

		}
		out.close();

	}

}

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

import gnu.trove.TDoubleFunction;
import gnu.trove.TIntDoubleHashMap;
import gnu.trove.TObjectDoubleHashMap;

import java.util.List;

import de.dfki.km.perspecting.obie.model.Document;
import de.dfki.km.perspecting.obie.model.SemanticEntity;
import de.dfki.km.perspecting.obie.model.Token;
import de.dfki.km.perspecting.obie.model.TokenSequence;

public class TermFrequencyBasedRating implements RatingMetric {

	@Override
	public TIntDoubleHashMap getRating(Document document, List<TokenSequence<SemanticEntity>> entities) {
		TIntDoubleHashMap m = new TIntDoubleHashMap();
		
		for (TokenSequence<SemanticEntity> ts : entities) {
			TObjectDoubleHashMap<String> histogram = new TObjectDoubleHashMap<String>();
			for (Token t : ts.getTokens()) {
				histogram.put(t.toString(), 0.0);
			}

			for (Token t : document) {
				histogram.increment(t.toString());
			}

			final double docCard = document.getTokens().size();
			histogram.transformValues(new TDoubleFunction() {
				@Override
				public double execute(double value) {
					return value / docCard;
				}
			});

			m.put(ts.getValue().getSubjectIndex(), histogram.getValues()[0]);
			
		}
		
		return m;
	}
	
}

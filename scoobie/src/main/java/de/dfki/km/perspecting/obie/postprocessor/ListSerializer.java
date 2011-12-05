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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import de.dfki.km.perspecting.obie.connection.KnowledgeBase;
import de.dfki.km.perspecting.obie.model.Document;
import de.dfki.km.perspecting.obie.model.DoubleMatrix;
import de.dfki.km.perspecting.obie.model.TIntDoubleTuple;

public class ListSerializer implements Serializer {
	
	private final int rating;
	
	public ListSerializer(int rating) {
		this.rating = rating;
	}
	
	
	@Override
	public Reader serialize(Document document, KnowledgeBase kb)
			throws Exception {
		
		DoubleMatrix matrix = document.getRelevanceScores();
		
		Integer[] keys = matrix.getRowKeys();
		double[] ratings = matrix.getColumn(rating);

		List<TIntDoubleTuple> ranking = new ArrayList<TIntDoubleTuple>();

		for (int i = 0; i < keys.length; i++) {
			ranking.add(new TIntDoubleTuple(keys[i], ratings[i]));
		}

		Collections.sort(ranking, new Comparator<TIntDoubleTuple>() {
			@Override
			public int compare(TIntDoubleTuple o1, TIntDoubleTuple o2) {
				return Double.compare(o2.getValue(), o1.getValue());
			}
		});
		
		
		StringBuilder b = new StringBuilder();
		for(TIntDoubleTuple t : ranking) {
			b.append(String.format("%s\n", kb.getURI(t.getKey())));
		}

		
		return new StringReader(b.toString());
	}

}

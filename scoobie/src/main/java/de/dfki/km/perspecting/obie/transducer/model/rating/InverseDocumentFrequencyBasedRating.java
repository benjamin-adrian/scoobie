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

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;

import de.dfki.km.perspecting.obie.corpus.TextCorpus;
import de.dfki.km.perspecting.obie.model.Document;
import de.dfki.km.perspecting.obie.model.SemanticEntity;
import de.dfki.km.perspecting.obie.model.TokenSequence;

public class InverseDocumentFrequencyBasedRating implements RatingMetric {

	private IndexSearcher tfIdfIndex;

	public InverseDocumentFrequencyBasedRating(TextCorpus corpus, File directory)
			throws Exception {
		this.tfIdfIndex = corpus.getLuceneIndex(directory, false);
	}

	@Override
	public TIntDoubleHashMap getRating(Document document,
			List<TokenSequence<SemanticEntity>> entities) {

		TIntDoubleHashMap m = new TIntDoubleHashMap();
		TObjectDoubleHashMap<String> histogram = new TObjectDoubleHashMap<String>();

		try {
			Term[] l_terms = new Term[entities.size()];

			for (int i = 0; i < entities.size(); i++) {
				l_terms[i] = new Term("text", entities.get(i).toString());
			}

			int[] df = tfIdfIndex.docFreqs(l_terms);

			for (int i = 0; i < entities.size(); i++) {
				histogram.put(entities.get(i).toString(), ((double) df[i] + 1)
						/ tfIdfIndex.getIndexReader().numDocs());
			}

			histogram.transformValues(new TDoubleFunction() {
				@Override
				public double execute(double value) {
					return Math.log(1.0 / value);
				}
			});
		} catch (IOException e) {
			e.printStackTrace();
		}

		for (TokenSequence<SemanticEntity> ts : entities) {
			String word = ts.toString();
			double idfScore = histogram.get(word);
			m.put(ts.getValue().getSubjectIndex(), idfScore);
		}

		return m;
	}

	@Override
	protected void finalize() throws Throwable {
		this.tfIdfIndex.close();
		super.finalize();
	}

}

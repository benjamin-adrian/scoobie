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

import gnu.trove.TIntDoubleHashMap;
import gnu.trove.TIntHashSet;

import java.io.BufferedReader;
import java.io.Reader;
import java.util.Arrays;
import java.util.List;

import de.dfki.km.perspecting.obie.connection.KnowledgeBase;
import de.dfki.km.perspecting.obie.model.Document;
import de.dfki.km.perspecting.obie.model.DoubleMatrix;
import de.dfki.km.perspecting.obie.model.SemanticEntity;
import de.dfki.km.perspecting.obie.model.TokenSequence;
import de.dfki.km.perspecting.obie.transducer.model.rating.RatingMetric;
import de.dfki.km.perspecting.obie.workflow.Transducer;

public class RelevanceRating extends Transducer {

	private RatingMetric[] ratings;
	private int[][] fusions;

	public RelevanceRating(RatingMetric[] ratings, int[]... fusions) {
		this.fusions = fusions;
		this.ratings = ratings;
	}

	@Override
	public void transduce(Document doc, KnowledgeBase kb) throws Exception {

		DoubleMatrix matrix = new DoubleMatrix();
		rate(doc, matrix);
		int index = ratings.length;
		
//		System.out.println(matrix.toColt().toString());
		for(int i=0; i < ratings.length; i++) {
			matrix.normalize(i);
		}
		
//		System.out.println(matrix.toColt().toString());
		for (int[] fusion : fusions) {
			matrix.fuseRatings(index, DoubleMatrix.PRODUCT, fusion);
			matrix.normalize(index);
			index++;
		}
//		System.out.println(matrix.toColt().toString());
		doc.setRelevanceScores(matrix);

	}

	private void rate(Document document, DoubleMatrix matrix) {

		List<TokenSequence<SemanticEntity>> entities = document
				.getResolvedSubjects();

		for (int metric = 0; metric < ratings.length; metric++) {
			TIntDoubleHashMap map = ratings[metric].getRating(document, entities);
			for (int node : map.keys()) {
				matrix.add(metric,node, map.get(node));
			}
		}
	}


	@Override
	public String compare(Document document, KnowledgeBase kb, Reader gt)
			throws Exception {

		BufferedReader br = new BufferedReader(gt);

		TIntHashSet gtUris = new TIntHashSet();
		TIntHashSet found = new TIntHashSet();

		for (String line = br.readLine(); line != null; line = br.readLine()) {
			try {
				gtUris.add(kb.getUriIndex(line));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		for (TokenSequence<SemanticEntity> ts : document.getResolvedSubjects()) {
			found.add(ts.getValue().getSubjectIndex());
		}

		StringBuilder outputBuilder = new StringBuilder();

		DoubleMatrix matrix = document.getRelevanceScores();

		System.out.println(matrix.toColt().toString());
		
		String[] doc = document.getUri().toString().split("/");

		// printMatrixDumps(matrix, doc);

		for (int i = 0; i < ratings.length; i++) {
			double map = matrix.averagePrecision(gtUris.toArray(), i);
			outputBuilder.append(String.format(
					"%s\t%s\t%1.5f\t%s\t%s\t%1.5f\t%1.5f\n",
					doc[doc.length - 1], ratings[i].getClass().getSimpleName(), map, gtUris.size(), found
							.size(), matrix.precision(gtUris.toArray(), i),
					matrix.recall(gtUris.toArray(), i)));
		}
		
		for (int i = 0; i < fusions.length; i++) {
			double map = matrix.averagePrecision(gtUris.toArray(), ratings.length + i);
			outputBuilder.append(String.format(
					"%s\t%s\t%1.5f\t%s\t%s\t%1.5f\t%1.5f\n",
					doc[doc.length - 1], Arrays.toString(fusions[i]), map, gtUris.size(), found
							.size(), matrix.precision(gtUris.toArray(), i),
					matrix.recall(gtUris.toArray(), i)));
		}

		return outputBuilder.toString();
	}



}

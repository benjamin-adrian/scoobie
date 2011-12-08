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
import gnu.trove.TIntIntHashMap;
import gnu.trove.TIntObjectHashMap;
import gnu.trove.TObjectIntHashMap;

import java.io.Reader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.logging.Logger;

import cern.colt.function.IntIntDoubleFunction;
import cern.colt.matrix.DoubleMatrix1D;
import cern.colt.matrix.DoubleMatrix2D;
import cern.colt.matrix.doublealgo.Statistic;
import cern.colt.matrix.linalg.Algebra;
import de.dfki.km.perspecting.obie.connection.KnowledgeBase;
import de.dfki.km.perspecting.obie.connection.RemoteCursor;
import de.dfki.km.perspecting.obie.model.Document;
import de.dfki.km.perspecting.obie.model.DoubleMatrix;
import de.dfki.km.perspecting.obie.model.RDFEdge;
import de.dfki.km.perspecting.obie.model.SemanticEntity;
import de.dfki.km.perspecting.obie.model.TokenSequence;
import de.dfki.km.perspecting.obie.workflow.Transducer;
import edu.uci.ics.jung.graph.DirectedGraph;
import edu.uci.ics.jung.graph.DirectedSparseGraph;

/**
 * @author adrian
 * @version 0.1
 * @since 17.03.2010
 * 
 */
public class FactRecommender extends Transducer {

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * de.dfki.km.perspecting.obie.workflow.tasks.FactSelection#extractFacts
	 * (java.util.List, java.util.List,
	 * de.dfki.km.perspecting.obie.template.FilterContext,
	 * de.dfki.km.perspecting.obie.model.Model)
	 */

	private final Logger log = Logger
			.getLogger(FactRecommender.class.getName());

	@Override
	public void transduce(Document document, KnowledgeBase kb)
			throws Exception {

		TIntHashSet instances = new TIntHashSet();
		TIntIntHashMap classification = new TIntIntHashMap();

		for (TokenSequence<SemanticEntity> ts : document.getResolvedSubjects()) {
			int s = ts.getValue().getSubjectIndex();
			if (!instances.contains(s)) {
				instances.add(s);
			}
		}

		TIntObjectHashMap<TIntHashSet> dClassifications = new TIntObjectHashMap<TIntHashSet>();

		RemoteCursor rs = kb.getRDFTypesForInstances(instances.toArray());
		while (rs.next()) {
			TIntHashSet types = dClassifications.get(rs.getInt(1));
			if (types == null) {
				types = new TIntHashSet();
				dClassifications.put(rs.getInt(1), types);
			}
			types.add(rs.getInt(2));
		}

		for (int i : instances.toArray()) {
			if (dClassifications.contains(i)) {
				int[] stypes = dClassifications.get(i).toArray();
				if (stypes.length > 0) {
					int type_s = kb.getCluster(stypes);
					classification.put(i, type_s);
				}
			}
		}

		DirectedSparseGraph<Integer, RDFEdge> predictedGraph = new DirectedSparseGraph<Integer, RDFEdge>();
		document.setPredictionGraph(predictedGraph);

		transduceMarkov(document, kb, classification, 1, predictedGraph);

	}

	/**
	 * @param document
	 * @param matrix
	 * @param po
	 * @param revPO
	 */
	private TIntHashSet populateMatrix(Document document, DoubleMatrix matrix,
			TObjectIntHashMap<String> po, TIntObjectHashMap<String> revPO,
			int type, HashSet<Integer> clusters) {
		int poIndex = 0;

		TIntHashSet types = new TIntHashSet();

		for (int s : document.getGraph().getVertices()) {
			for (RDFEdge edge : document.getGraph().getOutEdges(s)) {
				int o = document.getGraph().getDest(edge);

				if (!po.contains(edge.getPredicate() + "-" + o)) {
					po.put(edge.getPredicate() + "-" + o, poIndex);
					revPO.put(poIndex, edge.getPredicate() + "-" + o);
					poIndex++;

					if (edge.getPredicate() == type && clusters.contains(o)) {
						types.add(po.get(edge.getPredicate() + "-" + o));
					}
				}

				matrix.add(s, po.get(edge.getPredicate() + "-" + o), 1.0);
			}
		}

		return types;
	}

	@Override
	public String compare(Document document, KnowledgeBase kb, Reader gt)
			throws Exception {

		TIntHashSet instances = new TIntHashSet();
		TIntIntHashMap classification = new TIntIntHashMap();

		for (TokenSequence<SemanticEntity> ts : document.getResolvedSubjects()) {
			int s = ts.getValue().getSubjectIndex();
			if (!instances.contains(s)) {
				instances.add(s);
			}
		}

		TIntObjectHashMap<TIntHashSet> dClassifications = new TIntObjectHashMap<TIntHashSet>();

		RemoteCursor rs = kb.getRDFTypesForInstances(instances.toArray());
		while (rs.next()) {
			TIntHashSet types = dClassifications.get(rs.getInt(1));
			if (types == null) {
				types = new TIntHashSet();
				dClassifications.put(rs.getInt(1), types);
			}
			types.add(rs.getInt(2));
		}

		for (int i : instances.toArray()) {
			if (dClassifications.contains(i)) {
				int[] stypes = dClassifications.get(i).toArray();
				if (stypes.length > 0) {
					int type_s = kb.getCluster(stypes);
					classification.put(i, type_s);
				}
			}
		}

		return compareRecommender(document, kb, classification);
		// compareMarkov(document, kb, classification, 1)
		// + compareMarkov(document, kb, classification, 2)
		// + compareMarkov(document, kb, classification, 3)
		// + compareMarkov(document, kb, classification, 4)
		// + compareMarkov(document, kb, classification, 5);
	}

	private void transduceMarkov(Document document, KnowledgeBase kb,
			TIntIntHashMap classification, int kBest,
			DirectedGraph<Integer, RDFEdge> graph) throws Exception {

		kb.calculateMarkovChain(new int[] {}, 10);


		for (int subject : classification.keys()) {
			for (int object : classification.keys()) {
				if (object != subject) {
					int type_s = classification.get(subject);
					int type_o = classification.get(object);
					for (double[] pp : kb.getMaxMarkovProbability(type_s, type_o,
							kBest)) {
						graph.addEdge(new RDFEdge((int) pp[0], pp[1]), subject,
								object);
					}
				}
			}
		}

	}

	/**
	 * @param document
	 * @param kb
	 * @return
	 * @throws Exception
	 */
	private String compareMarkov(Document document, KnowledgeBase kb,
			TIntIntHashMap classification, int kBest) throws Exception {

		TObjectIntHashMap<String> po = new TObjectIntHashMap<String>();
		TIntObjectHashMap<String> revPO = new TIntObjectHashMap<String>();

		int poIndex = 0;

		ArrayList<int[]> spoList = new ArrayList<int[]>();

		for (int s : document.getGraph().getVertices()) {
			for (RDFEdge edge : document.getGraph().getOutEdges(s)) {
				int o = document.getGraph().getDest(edge);

				if (!po.contains(edge.getPredicate() + "-" + o)) {
					po.put(edge.getPredicate() + "-" + o, poIndex);
					revPO.put(poIndex, edge.getPredicate() + "-" + o);
					poIndex++;
				}

				spoList.add(new int[] { s, edge.getPredicate(), o });
			}
		}

		ArrayList<int[]> l_spoList = new ArrayList<int[]>(spoList);

		int[] removed;

		double correct = 0;
		double unknown = 0;

		int sum = 0;

		kb.calculateMarkovChain(new int[] {}, 10);
		
		// System.out.println(classification.keys().length);
		for (int k = 0; k < spoList.size(); k++) {
			if ((classification.contains(l_spoList.get(k)[0]) && classification
					.contains(l_spoList.get(k)[2]))) {
				sum++;
				removed = l_spoList.remove(k);
				for (int subject : classification.keys()) {
					for (int object : classification.keys()) {
						if (object != subject) {
							int type_s = classification.get(subject);
							int type_o = classification.get(object);
							for (double[] pp : kb.getMaxMarkovProbability(type_s,
									type_o, kBest)) {
								int predicate = (int) pp[0];
								double cardinality = kb.getSubjectCardinality(predicate);
								boolean exists = false;
								for (RDFEdge e : document.getGraph()
										.findEdgeSet(subject, object)) {
									if (e.getPredicate() == predicate) {
										exists = true;
									}
								}

								int l_card = 0;
								for (RDFEdge e : document.getGraph()
										.getOutEdges(subject)) {
									if (e.getPredicate() == predicate) {
										l_card++;
									}
								}

								if (removed[1] == predicate
										&& removed[0] == subject
										&& removed[2] == object) {
									if (l_card - 1 <= Math.ceil(cardinality)) {
										correct++;
									}
								} else {
									if (!exists
											&& l_card <= Math.ceil(cardinality)) {
										unknown++;
									}
								}
							}

						}

					}
				}
				l_spoList = new ArrayList<int[]>(spoList);
			}
		}

		double accuracy = correct / sum;
		double fallout = unknown / sum;

		if (sum == 0) {
			return String.format("%s\t%s\t%s\t%s\n", "markov" + kBest, document
					.getUri(), "unconnected", "unconnected");
		} else {
			return String.format("%s\t%s\t%1.5f\t%1.5f\n", "markov" + kBest,
					document.getUri(), accuracy, fallout);

		}
	}

	/**
	 * @param document
	 * @param kb
	 * @return
	 * @throws Exception
	 */
	private String compareRecommender(Document document, KnowledgeBase kb,
			TIntIntHashMap classification) throws Exception {

		final DoubleMatrix matrix = new DoubleMatrix();

		TObjectIntHashMap<String> po = new TObjectIntHashMap<String>();
		TIntObjectHashMap<String> revPO = new TIntObjectHashMap<String>();

		int poIndex = 0;

		ArrayList<int[]> spoList = new ArrayList<int[]>();

		int type = kb
				.getUriIndex("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");

		final TIntHashSet types = new TIntHashSet();

		final TIntHashSet clusters = new TIntHashSet(kb.getClusters());

		for (int s : document.getGraph().getVertices()) {
			for (RDFEdge edge : document.getGraph().getOutEdges(s)) {
				int o = document.getGraph().getDest(edge);

				if (!po.contains(edge.getPredicate() + "-" + o)) {
					po.put(edge.getPredicate() + "-" + o, poIndex);
					revPO.put(poIndex, edge.getPredicate() + "-" + o);

					if (edge.getPredicate() == type && clusters.contains(o)) {
						types.add(po.get(edge.getPredicate() + "-" + o));
					}

					poIndex++;
				}

				spoList.add(new int[] { s, edge.getPredicate(), o });
			}
		}

		ArrayList<int[]> l_spoList = new ArrayList<int[]>(spoList);

		int[] removed;

		double correct = 0;
		double unknown = 0;

		int sum = 0;
		kb.calculateMarkovChain(new int[] {}, 10);

		for (int k = 0; k < spoList.size(); k++) {

			if ((classification.contains(l_spoList.get(k)[0]) && classification
					.contains(l_spoList.get(k)[2]))) {
				sum++;
				removed = l_spoList.remove(k);

				for (int[] spo : l_spoList) {
					matrix.add(po.get(spo[1] + "-" + spo[2]), spo[0], 1.0);
				}

				DoubleMatrix2D mc = matrix.toColt();
				for (int c = 0; c < mc.columns(); c++) {
					DoubleMatrix1D col = mc.viewColumn(c);
					if (col.cardinality() == 1) {
						col.assign(0);
					}
				}
				// DoubleMatrix2D sim =
				// matrix.cosineSimilarity(types.toArray());
				DoubleMatrix2D sim = Statistic.correlation(
						Statistic.covariance(Algebra.DEFAULT.transpose(matrix
								.toColt()))).forEachNonZero(
						new IntIntDoubleFunction() {

							@Override
							public double apply(int arg0, int arg1, double arg2) {
								int ty = 0;
								for (int t : types.toArray()) {
									ty += matrix.get(t, arg0)
											* matrix.get(t, arg1);
								}
								return arg2 < 0 || ty == 0 ? 0 : arg2;
							}
						});

				DoubleMatrix2D p = Algebra.DEFAULT.transpose(matrix
						.predictValuesByCosine(sim, mc));

				// System.out.println(matrix.toColt().toString());
				// System.out.println(sim);
				// System.out.println(p.toString());

				for (int i = 0; i < matrix.getColKeys().length; i++) {
					DoubleMatrix1D row = p.viewRow(i);
					for (int j = 0; j < row.size(); j++) {
						if (row.get(j) != 0) {
							try {
								String[] predicateObject = revPO.get(
										matrix.getColKeys()[i]).split("-");
								int predicate = Integer
										.parseInt(predicateObject[0]);
								int object = Integer
										.parseInt(predicateObject[1]);
								int subject = matrix.getRowKeys()[j];

								if (subject != object) {
									if ((classification.contains(subject) && classification
											.contains(object))) {

										double pr = 0.00;

										int type_s = classification
												.get(subject);
										int type_o = classification.get(object);
										pr = kb.getMarkovProbability(type_s,
												predicate, type_o);
										if (pr == 0) {
											pr = 0.000001;
										}

										if (pr * row.get(j) > 0) {
											if (removed[0] == subject
													&& removed[1] == predicate
													&& removed[2] == object) {
												correct++;
											} else {
												unknown++;
											}

										}
									}
								}
							} catch (Exception e) {
								e.printStackTrace();
							}
						}
					}
				}
			}
			l_spoList = new ArrayList<int[]>(spoList);
		}

		double accuracy = correct / sum;
		double fallout = unknown / sum;

		if (sum == 0) {
			return String.format("%s\t%s\t%s\t%s\n", "reco", document
					.getUri(), "unconnected", "unconnected");
		} else {
			return String.format("%s\t%s\t%1.5f\t%1.5f\n", "reco", document
					.getUri(), accuracy, fallout);
		}
	}

}

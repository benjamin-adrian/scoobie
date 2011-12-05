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

import java.io.BufferedReader;
import java.io.Reader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.logging.Logger;

import de.dfki.km.perspecting.obie.connection.KnowledgeBase;
import de.dfki.km.perspecting.obie.connection.ResultSetCallback;
import de.dfki.km.perspecting.obie.model.Document;
import de.dfki.km.perspecting.obie.model.DoubleMatrix;
import de.dfki.km.perspecting.obie.model.RDFEdge;
import de.dfki.km.perspecting.obie.model.SemanticEntity;
import de.dfki.km.perspecting.obie.model.TokenSequence;
import de.dfki.km.perspecting.obie.transducer.model.disambiguation.AmbiguityResolver;
import de.dfki.km.perspecting.obie.workflow.Transducer;
import edu.uci.ics.jung.algorithms.cluster.WeakComponentClusterer;
import edu.uci.ics.jung.graph.DirectedGraph;
import edu.uci.ics.jung.graph.DirectedSparseGraph;
import gnu.trove.TIntHashSet;
import gnu.trove.TIntIntHashMap;
import gnu.trove.TIntObjectHashMap;

public class EntityDisambiguation extends Transducer {

	private final Logger log = Logger.getLogger(EntityDisambiguation.class
			.getName());

	private final AmbiguityResolver[] resolver;

	public EntityDisambiguation(AmbiguityResolver[] resolver) {
		this.resolver = resolver;
	}

	/**
	 * Create a directed graph in which an edge assigns a literal to a subject.
	 * 
	 * @param record
	 * @param componentLiterals
	 * @param vertexLabels
	 *            Map to fill in plain URIs and literals for rendering the
	 *            graph.
	 * @return
	 */
	private DirectedGraph<Integer, RDFEdge> populateGraphWithLiterals(
			Document record, Map<String, Set<Integer>> componentLiterals,
			Map<Integer, String> vertexLabels) {

		DirectedGraph<Integer, RDFEdge> graph = new DirectedSparseGraph<Integer, RDFEdge>();

		for (TokenSequence<SemanticEntity> ts : record.getResolvedSubjects()) {
			int index = ts.getValue().getSubjectIndex();
			String uri = ts.getValue().getSubjectURI();
			vertexLabels.put(index, uri);
			
			vertexLabels.put(-ts.getValue().getLiteralValueIndex(), ts.toString().toLowerCase(Locale.US));

			String phrase = ts.toString().toLowerCase(Locale.US);

			for (Integer key : componentLiterals.get(phrase)) {
				graph.addEdge(new RDFEdge(-1), key, index);
			}

//			for (Token p : ts.getTokens()) {
//				for (Integer key : componentLiterals.get(p.toString())) {
//					graph.addEdge(new RDFEdge(-1), key, index);
//				}
//			}
		}
		return graph;
	}

	@Override
	public void transduce(Document doc, KnowledgeBase kb)
			throws Exception {

		doc.setAmbiguityScores(new DoubleMatrix());
		Map<String, Set<Integer>> componentLiterals = splitLiteralComponents(doc);
		Map<Integer, String> vertexLabels = new HashMap<Integer, String>();

		DirectedGraph<Integer, RDFEdge> graph = populateGraphWithLiterals(doc,
				componentLiterals, vertexLabels);
		doc.setGraph(graph);

		doc.setLiteralsSubjectPairs(groupPairs(graph));

//		Set<Set<Integer>> literalSubjectPairs = doc.getLiteralSubjectPairs();

		populateGraphWithRelations(kb, doc, vertexLabels);

		// System.out.println(graph.getEdgeCount());
		pruneGraph(vertexLabels, graph);
		// System.out.println(graph.getEdgeCount());

		// serializeGraph(doc, vertexLabels, new HashMap<RDFEdge, String>());

		for (int i = 1; i < resolver.length; i++) {
			TIntObjectHashMap<TIntHashSet> resolvedSubjects = resolver[i]
					.resolve(doc.getGraph(), doc.getLiteralSubjectPairs(), doc, kb);

			for (int cs : resolvedSubjects.get(1).toArray()) {
				doc.getAmbiguityScores().add(i, cs, 1);
			}
			for (int cs : resolvedSubjects.get(0).toArray()) {
				doc.getAmbiguityScores().add(i, cs, -1);
			}
		}

//		doc.getAmbiguityScores().standardizeGauss();
//
//		doc.getAmbiguityScores().fuseRatings(resolver.length,
//				DoubleMatrix.PRODUCT, resolver.length-1, resolver.length-2);

		TIntObjectHashMap<TIntHashSet> resolvedSubjects = resolve(doc, resolver.length-1);

		prune(doc, vertexLabels, graph, doc.getLiteralSubjectPairs(),
				resolvedSubjects.get(0).toArray());

		//
		// ScoobieLogging.log(ScoobieLogging.UNKNOWN, doc.getSource().getUri(),
		// "pruning", log);
		//

	}

	/**
	 * @param vertexLabels
	 * @param graph
	 */
	private void pruneGraph(Map<Integer, String> vertexLabels,
			DirectedGraph<Integer, RDFEdge> graph) {
		HashSet<Integer> leafs = new HashSet<Integer>();
		for (int v : graph.getVertices()) {
			if (v < 0 || !vertexLabels.containsKey(v)
					&& graph.getInEdges(v).size() <= 1
					&& graph.getOutEdges(v).size() <= 1) {
				leafs.add(v);
			}
		}

		for (int s : leafs) {
			graph.removeVertex(s);
		}
	}

	/**
	 * @param doc
	 * @param literalSubjectPairs
	 * @return
	 */
	private TIntObjectHashMap<TIntHashSet> resolve(Document doc, int matrixCol) {

		TIntObjectHashMap<TIntHashSet> resolvedSubjects = new TIntObjectHashMap<TIntHashSet>();

		resolvedSubjects.put(0, new TIntHashSet());
		resolvedSubjects.put(1, new TIntHashSet());

		for (Set<Integer> c : doc.getLiteralSubjectPairs()) {
			Set<Integer> literals = new HashSet<Integer>();
			Set<Integer> subjects = new HashSet<Integer>();
			for (int i : c) {
				if (i < 0) {
					literals.add(i);
				} else {
					subjects.add(i);
				}
			}

			int maxInstance = -1;
			double maxValue = Double.NEGATIVE_INFINITY;

			for (int instance : subjects) {
				double value = doc.getAmbiguityScores()
						.get(matrixCol, instance);
				if (value > maxValue) {
					maxInstance = instance;
					maxValue = value;
				}
			}

			for (int s : subjects) {
				if (s != maxInstance) {
					resolvedSubjects.get(0).add(s);
				} else {
					resolvedSubjects.get(1).add(s);
				}
			}
		}
		return resolvedSubjects;
	}

	/**
	 * @param record
	 * @param graph
	 * @throws Exception
	 */
	private void populateGraphWithRelations(KnowledgeBase rdf, Document doc,
			Map<Integer, String> vertexLabels) throws Exception {

		TreeMap<Integer, TIntHashSet> ambiguityRanking = new TreeMap<Integer, TIntHashSet>();
		TIntIntHashMap initialNodes = new TIntIntHashMap();

		for (Set<Integer> componentGraph : doc.getLiteralSubjectPairs()) {
			TIntHashSet uriRefs = new TIntHashSet();
			TIntHashSet literals = new TIntHashSet();

			for (Integer component : componentGraph) {
				if (component < 0) {// node is a literal
					literals.add(-component);
				} else { // node is a URI
					uriRefs.add(component);
					initialNodes.adjustOrPutValue(component, 1, 1);
					doc.getGraph().addVertex(component); // add to graph
				}
			}

			// populate
			TIntHashSet s_uriRefs = ambiguityRanking.get(uriRefs.size());
			if (s_uriRefs == null) {
				s_uriRefs = new TIntHashSet();
				ambiguityRanking.put(uriRefs.size(), s_uriRefs);
			} else {
				s_uriRefs.addAll(uriRefs.toArray());
			}
		}

		TIntHashSet blackListedRefs = new TIntHashSet();

		int numberOfAmbiguousSets = analyseAmbiguities(doc, blackListedRefs);

		TIntHashSet newRefs = new TIntHashSet();

		for (int card : ambiguityRanking.keySet()) {
			TIntHashSet clearRefs = new TIntHashSet(ambiguityRanking.get(card)
					.toArray());
			clearRefs.removeAll(blackListedRefs.toArray());
			if (clearRefs.size() > 0) {
				newRefs.addAll(traverseForward(rdf, clearRefs.toArray(),
						doc.getGraph()).toArray());
				blackListedRefs.addAll(clearRefs.toArray());
			}

			numberOfAmbiguousSets = analyseAmbiguities(doc, blackListedRefs);

			if (numberOfAmbiguousSets == 0) {
				break;
			}
		}

		for (Set<Integer> c : doc.getLiteralSubjectPairs()) {
			TIntHashSet subjects = new TIntHashSet();
			for (int i : c) {
				if (i >= 0) {
					subjects.add(i);
				}
			}

			if (!isConnected(doc, subjects.toArray())) {
				newRefs.addAll(traverseBackward(rdf, subjects.toArray(),
						doc.getGraph()).toArray());
			}
		}
		numberOfAmbiguousSets = analyseAmbiguities(doc, blackListedRefs);

	}

	/**
	 * @param doc
	 * @param blackListedRefs
	 * @return
	 */
	private int analyseAmbiguities(Document doc, TIntHashSet blackListedRefs) {
		int ambiguousSets = 0;

		for (Set<Integer> c : doc.getLiteralSubjectPairs()) {
			TIntHashSet subjects = new TIntHashSet();
			for (int i : c) {
				if (i >= 0) {
					subjects.add(i);
				}
			}

			if (isConnected(doc, subjects.toArray())) {
				for (int i : subjects.toArray()) {
					blackListedRefs.add(i);
				}
			} else {
				ambiguousSets++;
			}
		}
		return ambiguousSets;
	}

	/**
	 * @param doc
	 * @param subjects
	 * @return
	 */
	private boolean isConnected(Document doc, int[] subjects) {
		int count = 0;

		for (int i : subjects) {
			for (int n : doc.getGraph().getNeighbors(i)) {
				if (n >= 0)
					count++;
			}
		}
		return count > 0;
	}

	/**
	 * @param document
	 * @param vertexLabels
	 * @param graph
	 * @param literalSubjectPairs
	 * @param toBeClearedSubjects
	 * @throws Exception
	 */
	private void prune(Document document, Map<Integer, String> vertexLabels,
			DirectedGraph<Integer, RDFEdge> graph,
			Set<Set<Integer>> literalSubjectPairs, int[] toBeClearedSubjects)
			throws Exception {
		// for (Set<Integer> component : literalSubjectPairs) {
		// for (int i : toBeClearedSubjects) {
		// // component.remove(i);
		// }
		// }

		for (int s : toBeClearedSubjects) {
			graph.removeVertex(s);
			log.fine("removed subject: " + vertexLabels.get(s));
		}

		document.removeUnresolvedSubjects(toBeClearedSubjects);

	}

	/**
	 * @param graph
	 * @return
	 */
	private Set<Set<Integer>> groupPairs(DirectedGraph<Integer, RDFEdge> graph) {
		WeakComponentClusterer<Integer, RDFEdge> componentCluster = new WeakComponentClusterer<Integer, RDFEdge>();
		Set<Set<Integer>> components = componentCluster.transform(graph);
		return components;
	}

	/**
	 * 
	 * Splits phrases into single tokens.
	 * 
	 * @param record
	 * @return
	 */
	private HashMap<String, Set<Integer>> splitLiteralComponents(Document record) {
		HashMap<String, Set<Integer>> componentLiterals = new HashMap<String, Set<Integer>>();
		for (TokenSequence<SemanticEntity> ts : record.getResolvedSubjects()) {
			String phrase = ts.toString().toLowerCase(Locale.US);

//			if (ts.getTokens().size() > 1) {
//				for (Token tString : ts.getTokens()) {
//					Set<Integer> s = componentLiterals.get(tString.toString());
//					if (s == null) {
//						s = new HashSet<Integer>();
//						componentLiterals.put(tString.toString(),
//								s);
//					}
//					s.add(-ts.getValue().getLiteralValueIndex());
//				}
//			}
			Set<Integer> s = componentLiterals.get(phrase);
			if (s == null) {
				s = new HashSet<Integer>();
				componentLiterals.put(phrase, s);
			}
			s.add(-ts.getValue().getLiteralValueIndex());
		}
		return componentLiterals;
	}

	private TIntHashSet traverseBackward(KnowledgeBase kb, int[] objects,
			DirectedGraph<Integer, RDFEdge> graph) throws Exception {

		TIntHashSet newReferences = new TIntHashSet();

		ResultSetCallback rs = kb.getIncomingRelations(objects);

		while (rs.getRs().next()) {
			int subject = rs.getRs().getInt(1);
			int predicate = rs.getRs().getInt(2);
			int object = rs.getRs().getInt(3);

			if (!graph.containsVertex(subject))
				newReferences.add(subject);

			try {
				graph.addEdge(new RDFEdge(predicate), subject, object);
			} catch (Exception e) {

			}

		}

		rs.close();

		return newReferences;
	}

	private TIntHashSet traverseForward(KnowledgeBase kb, int[] uriRefs,
			DirectedGraph<Integer, RDFEdge> graph) throws Exception {

		ResultSetCallback rs = kb.getOutgoingRelations(uriRefs);

		TIntHashSet newReferences = new TIntHashSet();

		int type = kb
				.getUriIndex("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");

		while (rs.getRs().next()) {
			int subject = rs.getRs().getInt(1);
			int predicate = rs.getRs().getInt(2);
			int object = rs.getRs().getInt(3);

			// means to add no links RDFS classes in this graph
			if (predicate != type) {
				if (!graph.containsVertex(object))
					newReferences.add(object);
				try {
					graph.addEdge(new RDFEdge(predicate), subject, object);
				} catch (Exception e) {

				}
			}
		}
		rs.close();
		return newReferences;
	}

	@Override
	public String compare(Document document, KnowledgeBase kb, Reader gt)
			throws Exception {

		BufferedReader br = new BufferedReader(gt);

		HashSet<String> gtUris = new HashSet<String>();

		for (String line = br.readLine(); line != null; line = br.readLine()) {
			gtUris.add(line);
		}

		StringBuilder outputBuilder = new StringBuilder();

		HashSet<String> ambUris = new HashSet<String>();
		HashSet<Integer> ambUriSet = new HashSet<Integer>();

		// System.out.println(document.getLiteralSubjectPairs());

		for (Set<Integer> c : document.getLiteralSubjectPairs()) {
			Set<Integer> literals = new HashSet<Integer>();
			Set<Integer> subjects = new HashSet<Integer>();
			for (int i : c) {
				if (i < 0) {
					literals.add(i);
				} else {
					subjects.add(i);
				}
			}

			if (subjects.size() > 1) {
//				System.out.println("Ambiguities for literals: ");
//				for (int l : literals) {
//					System.out.println(kb.getLiteral(-l));
//				}
//				System.out.println("Ambiguously resolved as: ");
				for (int s : subjects) {
					String uri = kb.getURI(s);
					ambUris.add(uri);
					ambUriSet.addAll(subjects);
//					System.out.println(uri);
				}
			}
		}

		gtUris.retainAll(ambUris);

		for (int i = 0; i < resolver.length; i++) {
			double foundRelevant = 0;
			double foundNotRelevant = 0;

			HashSet<String> foundUris = new HashSet<String>();
			TIntObjectHashMap<TIntHashSet> resolvedSubjects = resolve(document,
					i);

			// for (TokenSequence<SemanticEntity> semEnt : document
			// .getResolvedSubjects()) {
			for (int subject : resolvedSubjects.get(1).toArray()) {
				String foundURI = kb.getURI(subject);

				if (ambUriSet.contains(subject)) {
					if (!foundUris.contains(foundURI)) {
						if (gtUris.contains(foundURI)) {
							foundRelevant++;
						} else {
							foundNotRelevant++;
						}
						foundUris.add(foundURI);
					}
				}
			}

			double precision = foundRelevant
					/ (foundRelevant + foundNotRelevant);
			double recall = foundRelevant / gtUris.size();

			HashSet<String> missedUris = new HashSet<String>(gtUris);
			missedUris.removeAll(foundUris);

			HashSet<String> falseUris = new HashSet<String>(foundUris);
			falseUris.removeAll(gtUris);

			HashSet<String> trueUris = new HashSet<String>(foundUris);
			trueUris.retainAll(gtUris);

			String[] doc = document.getUri().toString().split("/");

			outputBuilder
					.append(String
							.format(
									resolver[i]
											+ "\turi\t%s\tP\t%1.5f\tR\t%1.5f\t|GT|\t%s\t|FOUND|\t%s\tmissed\t%s\ttrue\t%s\tfalse\t%s\n",
									doc[doc.length - 1], precision, recall,
									gtUris.size(), foundUris.size(), missedUris
											.toString(), trueUris.toString(),
									falseUris.toString()));
		}
		return outputBuilder.toString();

	}

}

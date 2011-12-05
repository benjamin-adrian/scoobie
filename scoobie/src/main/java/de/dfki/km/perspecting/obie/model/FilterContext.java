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

package de.dfki.km.perspecting.obie.model;

import gnu.trove.TIntHashSet;

import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;

import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;

import de.dfki.km.perspecting.obie.connection.KnowledgeBase;

/**
 * @author adrian
 * @version 0.1
 * @since 12.10.2009
 * 
 */
public class FilterContext {

	private final TIntHashSet datatypePropertyIndexFilter = new TIntHashSet();
	private final TIntHashSet objectPropertyIndexFilter = new TIntHashSet();
	private final TIntHashSet subjectIndexFilter = new TIntHashSet();
	private final TIntHashSet literalIndexFilter = new TIntHashSet();
	private final TIntHashSet typeIndexFilter = new TIntHashSet();

	private final Set<String> defaultGraphs = new HashSet<String>();
	private final Set<String> namedGraphs = new HashSet<String>();
	private final Set<String> contextGraphs = new HashSet<String>();

	private final String query;

	private String rdfGraphUri;
	private String predictedGraphUri;
	private String recognizedGraphUri;

	private final static Logger log = Logger.getLogger(FilterContext.class
			.getName());

	private final KnowledgeBase kb;

	/**
	 * This is the implementation of a filter generation algorithms that takes a
	 * SPARQL query as input
	 */
	private final QueryModelVisitorBase<Exception> qmvb = new QueryModelVisitorBase<Exception>() {

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * org.openrdf.query.algebra.helpers.QueryModelVisitorBase#meet(org.
		 * openrdf.query.algebra.StatementPattern)
		 */
		@Override
		public void meet(StatementPattern node) throws Exception {

			if (node.getContextVar() != null && node.getContextVar().hasValue()) {
				contextGraphs
						.add(node.getContextVar().getValue().stringValue());

				if (node.getContextVar().getValue().stringValue().endsWith("#recognized")) {
					lookupPredicate(node);
					lookupSubject(node);
					lookupObject(node);
				}

			}

		}

		private void lookupObject(StatementPattern node) {

			Var o = node.getObjectVar();
			Var p = node.getPredicateVar();

			if (o.hasValue()) {
				int index;

				if (o.getValue() instanceof URI) {

					if (p.getValue().stringValue().equals(
							RDF.TYPE.stringValue())) {
						try {
							index = kb.getUriIndex(o.getValue().stringValue());
							typeIndexFilter.add(index);
						} catch (Exception e) {

							log.warning("RDF Object" + o.getValue()
									+ " is not indexed in session model");
						}
					} else {
						try {
							index = kb.getUriIndex(o.getValue().stringValue());
							subjectIndexFilter.add(index);
						} catch (Exception e) {
							log.warning("RDF Object" + o.getValue()
									+ " is not indexed in session model");
						}

					}

				} else if (o.getValue() instanceof Literal) {
					try {

						index = kb.getLiteralIndex(o.getValue().stringValue());
						literalIndexFilter.add(index);
					} catch (Exception e) {
						log.warning("RDF Object" + o.getValue()
								+ " is not indexed in session model");
					}
				}
			}
		}

		private void lookupSubject(StatementPattern node) {
			Var s = node.getSubjectVar();

			if (s.hasValue()) {
				int index;
				try {
					index = kb.getUriIndex(s.getValue().stringValue());
					subjectIndexFilter.add(index);

				} catch (Exception e) {
					log.warning("RDF Subject" + s.getValue()
							+ " is not indexed in session model");

				}

			}
		}

		private void lookupPredicate(StatementPattern node) {
			Var p = node.getPredicateVar();
			if (p.hasValue()
					&& !p.getValue().stringValue().equals(RDF.TYPE.toString())) {
				int index;
				try {
					index = kb.getUriIndex(p.getValue().stringValue());
					int type = kb.getPropertyType(index);
					if (type == 1) {
						datatypePropertyIndexFilter.add(index);
						log.info("added filtering datatype property: " + index);

					} else if (type == 2) {
						objectPropertyIndexFilter.add(index);
						log.info("added filtering object property: " + index);
					}
				} catch (Exception e) {
					e.printStackTrace();
					log.warning("RDF Property" + p.getValue()
							+ " is not indexed in session model");
				}
			}
		}
	};

	public FilterContext(java.net.URI rdfGraphUri, String predictedGraphUri,
			String recognizedGraphUri, KnowledgeBase ontology, String query)
			throws Exception {
		this.kb = ontology;
		this.query = query;

		this.rdfGraphUri = rdfGraphUri.toString();
		this.predictedGraphUri = predictedGraphUri;
		this.recognizedGraphUri = recognizedGraphUri;

		log.info("RDF graph: " + rdfGraphUri);
		log.info("prediction graph: " + predictedGraphUri);
		log.info("recognition graph: " + recognizedGraphUri);
		
		
		if (this.query == null)
			return;

		SPARQLParser parser = new SPARQLParser();

		ParsedQuery pquery = parser.parseQuery(query, null);

		if (pquery.getDataset() != null) {
			for (URI uri : pquery.getDataset().getDefaultGraphs()) {
				defaultGraphs.add(uri.stringValue());
			}
		}

		if (pquery.getDataset() != null) {
			for (URI uri : pquery.getDataset().getNamedGraphs()) {
				namedGraphs.add(uri.stringValue());
			}
		}

		pquery.getTupleExpr().visit(qmvb);

	}

	/**
	 * @return the defaultgraphs
	 */
	public Set<String> getDefaultGraphs() {
		return defaultGraphs;
	}

	/**
	 * @return the predicateIndexFilter
	 */
	public TIntHashSet getDatatypePropertyIndexFilter() {
		return datatypePropertyIndexFilter;
	}

	/**
	 * @return the namedgraphs
	 */
	public Set<String> getNamedGraphs() {
		return namedGraphs;
	}

	public void infer(double markovProbability, double rating) throws Exception {
		// substitude classes wth clusters

		for (int objectProperty : objectPropertyIndexFilter.toArray()) {
			for (int[] clusters : kb.getConnectingClusters(objectProperty,
					markovProbability)) {
				typeIndexFilter.addAll(clusters);
				log.info("added class " + kb.getURI(clusters[0])
						+ " from resolving domain of "
						+ kb.getURI(objectProperty));
				log.info("added class " + kb.getURI(clusters[1])
						+ " from resolving range of "
						+ kb.getURI(objectProperty));
			}
		}

		for (int type : typeIndexFilter.toArray()) {
			int cluster = kb.getCluster(new int[] { type });
			if (type != cluster) {
				typeIndexFilter.remove(type);
				typeIndexFilter.add(cluster);
				log.info("substituted class " + kb.getURI(type)
						+ " with cluster " + kb.getURI(cluster));
			}
			int[] datatypeProperties = kb.getDatatypePropertyByClass(cluster,
					rating);

			for (int datatypeProperty : datatypeProperties) {
				if (datatypePropertyIndexFilter.add(datatypeProperty))
					log.info("added datatype property describing class "
							+ kb.getURI(cluster) + ": "
							+ kb.getURI(datatypeProperty));

			}
		}

	}

	/**
	 * @return the contextGraphs
	 */
	public Set<String> getContextGraphs() {
		return contextGraphs;
	}

	/**
	 * @return the predicatefilter
	 * @throws Exception
	 */
	public Set<String> getDatatypePropertyFilter() throws Exception {
		Set<String> s = new HashSet<String>();
		for (int value : datatypePropertyIndexFilter.toArray()) {
			s.add(kb.getURI(value));
		}

		return s;
	}

	/**
	 * @return the objectPropertyFilter
	 * @throws Exception
	 */
	public Set<String> getObjectPropertyFilter() throws Exception {
		Set<String> s = new HashSet<String>();
		for (int value : objectPropertyIndexFilter.toArray()) {
			s.add(kb.getURI(value));
		}

		return s;
	}

	/**
	 * @return the objectPropertyFilter
	 * @throws Exception
	 */
	public Set<String> getSubjectFilter() throws Exception {
		Set<String> s = new HashSet<String>();
		for (int value : subjectIndexFilter.toArray()) {
			s.add(kb.getURI(value));
		}

		return s;
	}

	/**
	 * @return the objectPropertyIndexFilter
	 */
	public TIntHashSet getObjectPropertyIndexFilter() {
		return objectPropertyIndexFilter;
	}

	public Set<String> getTypeFilter() throws Exception {
		Set<String> s = new HashSet<String>();
		for (int value : typeIndexFilter.toArray()) {
			s.add(kb.getURI(value));
		}

		return s;
	}

	public TIntHashSet getTypeIndexFilter() {
		return typeIndexFilter;
	}

	public Set<String> getLiteralFilter() throws Exception {
		Set<String> s = new HashSet<String>();
		for (int value : literalIndexFilter.toArray()) {
			s.add(kb.getLiteral(value));
		}

		return s;
	}

	public TIntHashSet getLiteralIndexFilter() {
		return literalIndexFilter;
	}

}

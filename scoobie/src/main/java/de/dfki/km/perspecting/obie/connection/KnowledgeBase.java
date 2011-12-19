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

package de.dfki.km.perspecting.obie.connection;

import gnu.trove.TIntDoubleHashMap;
import gnu.trove.TIntObjectHashMap;

import java.io.InputStream;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import de.dfki.km.perspecting.obie.corpus.TextCorpus;
import de.dfki.km.perspecting.obie.model.DoubleMatrix;
import de.dfki.km.perspecting.obie.transducer.model.LiteralHashing;
import de.dfki.km.perspecting.obie.transducer.model.SuffixArray;
import de.dfki.km.perspecting.obie.vocabulary.MediaType;
import de.dfki.km.perspecting.obie.workflow.Pipeline;

/**
 * 
 * @author adrian
 * @version 0.1
 */
public interface KnowledgeBase {

	/**
	 * Returns a ResultSet filled with existing values of a datatype property.
	 * 
	 * @param datatypePropertyFilter
	 *            List of datatype properties
	 * @param prefixes
	 *            List of hashed literal prefixes
	 **/
	RemoteCursor getDatatypePropertyValues(int[] datatypePropertyFilter, SuffixArray suffixArray) throws Exception;

	/**
	 * 
	 * Returns instances possessing datatype property values matching with
	 * entries of symbols.
	 * 
	 * @param symbols
	 *            Datatype property value pairs
	 */
	RemoteCursor getInstanceCandidates(Map<Integer, Set<Integer>> symbols)
			throws Exception;

	/**
	 * Returns the literal index for a given literal (no fuzzy match).
	 * 
	 * @param literal
	 *            Literal value
	 * @throws Exception
	 */
	int getLiteralIndex(String literal) throws Exception;

	/**
	 * Returns the index for a passed URI value.
	 * 
	 * @param uri
	 *            URI value as {@link String}
	 * 
	 * @throws Exception
	 */
	int getUriIndex(String uri) throws Exception;

	/**
	 * Returns the set of clustered classes.
	 * 
	 * @return Set of clustered class indexes as Array.
	 * @throws Exception
	 */
	int[] getClusters() throws Exception;

	/**
	 * Returns the URI value for a passed URI index.
	 * 
	 * @param index
	 *            Index of URI
	 * @return URI value as {@link String}
	 * @throws Exception
	 *             if index not in database
	 */
	String getURI(int index) throws Exception;

	/**
	 * Returns literal value of a passed literal index.
	 * 
	 * @param index
	 *            of a literal
	 * @return literal value as {@link String}
	 * @throws Exception
	 *             if index is not in database
	 */
	String getLiteral(int index) throws Exception;

	
	/**
	 * Returns outgoing edges for a given list of instances.
	 */
	RemoteCursor getOutgoingRelations(int[] instances) throws Exception;

	/**
	 * @param types
	 * @return
	 * @throws Exception
	 */
	int getCluster(int[] types) throws Exception;

	/**
	 * Returns incoming edges for a given list of instances.
	 */
	RemoteCursor getIncomingRelations(int[] instances) throws Exception;

	/**
	 * Returns the direct types for a given instance.
	 */
	RemoteCursor getRDFTypesForInstances(int[] subjects) throws Exception;

	/**
	 * Returns a list of contained RDF types.
	 * 
	 */
	RemoteCursor getRDFTypes() throws Exception;

	/**
	 * 
	 * Sorts a list of literal values.
	 * 
	 * @param list
	 * @param maxStringLength Maximal length of contained string.
	 * @return
	 * @throws Exception
	 */
	RemoteCursor dbSort(List<String> list, int maxStringLength)
			throws Exception;

	/**
	 * @param type
	 * @param limit
	 * @return
	 * @throws Exception
	 */
	RemoteCursor getInstancesOfTypes(int type, int limit) throws Exception;

	/**
	 * @param objectProperty
	 * @param threshold
	 * @return
	 * @throws Exception
	 */
	Collection<int[]> getConnectingClusters(int objectProperty, double threshold)
			throws Exception;

	/**
	 * @param property
	 * @return
	 * @throws Exception
	 */
	int getPropertyType(int property) throws Exception;

	/**
	 * @param cluster
	 * @param threshold
	 * @return
	 * @throws Exception
	 */
	int[] getDatatypePropertyByClass(int cluster, double threshold)
			throws Exception;

	/**
	 * @return
	 */
	URI getUri();

	void preprocessRdfData(InputStream[] datasets, MediaType rdfMimeType,
			MediaType fileMimeType, String absoluteBaseURI, LiteralHashing hashing) throws Exception;

	void calculateCardinalities() throws Exception;

	double getSubjectCardinality(int p) throws Exception;

	void calculateMarkovChain(int[] blackListedProperties, int sampleCount)
			throws Exception;

	List<double[]> getMaxMarkovProbability(int subject, int object, int k)
			throws Exception;

	double getMarkovProbability(int subject, int predicate, int object)
			throws Exception;

	TIntObjectHashMap<TIntObjectHashMap<double[]>> getCoverageAmbiguity()
			throws Exception;

	void calculateProperNameStatistics(TextCorpus corpus, Pipeline pipe)
			throws Exception;

	void clusterCorrelatingClasses(int samples, double biasThreshold,
			double pruningThreshold) throws Exception;

	DoubleMatrix getTypeCorrelations(int samples) throws Exception;
	
	void calculateRegexDistributions(String[] regexs) throws Exception;
	
	TIntDoubleHashMap getDatatypePropertiesForRegex(String regex) throws Exception;
	
	String[] getRegexs() throws Exception;

}
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

import java.io.File;
import java.io.InputStream;
import java.net.URI;
import java.sql.Connection;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import de.dfki.km.perspecting.obie.vocabulary.MediaType;

/**
 * an ontology and its instance base. It provides methods for querying
 * individuals with labels or indices.
 * 
 * To clarify the meaning here a description of our vocabulary:
 * 
 * An ontology can be seen as kind of a typed graph. The nodes in this graph are
 * either classes (TBOX, meaning "declarations"), instances (ABOX) or symbols
 * (LABOX). The edges are relations between the nodes and can be subdivided into
 * 
 * @author adrian
 * @version 0.1
 */
public interface KnowledgeBase {

	/**
	 * Returns the current session token.
	 * 
	 * @return session token
	 */
	public String getSession();

	/**
	 * Returns an open database connection.
	 * @deprecated All database requests should be implemented by the {@link KnowledgeBase}.
	 * @return connection to database
	 */
	Connection getConnection();

	/**
	 * Returns a ResultSet filled with existing values of a datatype property.
	 * 
	 * @param datatypePropertyFilter List of datatype properties
	 * @param prefixes List of hashed literal prefixes
	 **/
	ResultSetCallback getDatatypePropertyValues(
			int[] datatypePropertyFilter, int[] prefixes)
			throws Exception;

	/**
	 * 
	 * Returns instances possessing datatype property values matching with
	 * entries of symbols.
	 * 
	 * @param symbols Datatype property value pairs
	 */
	ResultSetCallback getInstanceCandidates(
			Map<Integer, Set<Integer>> symbols) throws Exception;

	/**
	 * Returns the literal index for a given literal (no fuzzy match).
	 * 
	 * @param literal Literal value
	 * @throws Exception
	 */
	int getLiteralIndex(String literal) throws Exception;

	
	/**
	 * Returns the index for a passed URI value.
	 * 
	 * @param uri URI value as {@link String}
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
	 * @param index Index of URI
	 * @return URI value as {@link String}
	 * @throws Exception if index not in database
	 */
	String getURI(int index) throws Exception;

	/**
	 * Returns literal value of a passed literal index.
	 * 
	 * @param index of a literal 
	 * @return literal value as {@link String}
	 * @throws Exception if index is not in database
	 */
	String getLiteral(int index) throws Exception;

	/**
	 * @param instance
	 * @param relation
	 * @return
	 * @throws Exception
	 */
	int[] getOutgoingRelations(int instance, int relation)
			throws Exception;

	/**
	 * @param instance
	 * @param relation
	 * @return
	 * @throws Exception
	 */
	int[] getIncomingRelations(int instance, int relation)
			throws Exception;

	/**
	 * @param instance
	 * @return
	 * @throws Exception
	 */
	Map<Integer, Set<Integer>> getOutgoingRelations(int instance)
			throws Exception;

	/**
	 * @param instances
	 * @return
	 * @throws Exception
	 */
	ResultSetCallback getOutgoingRelations(int[] instances)
			throws Exception;

	/**
	 * @param instance
	 * @return
	 * @throws Exception
	 */
	Map<Integer, Set<Integer>> getIncomingRelations(int instance)
			throws Exception;

	/**
	 * @param types
	 * @return
	 * @throws Exception
	 */
	int getCluster(int[] types) throws Exception;

	/**
	 * @param instances
	 * @return
	 * @throws Exception
	 */
	ResultSetCallback getIncomingRelations(int[] instances)
			throws Exception;


	/**
	 * Returns the direct types for a given instance.
	 * 
	 * @param instanceIndex
	 *            Index of instance
	 * @return A {@link List} of {@link Integer} index values about types.
	 */
	ResultSetCallback getRDFTypesForInstances(int [] subjects)
			throws Exception;
	
	/**
	 * @return
	 * @throws Exception
	 */
	ResultSetCallback getRDFTypes() throws Exception;

	/**
	 * @param index
	 * @param maxLength
	 * @return
	 * @throws Exception
	 */
	ResultSetCallback dbSort(List<CharSequence> index, int maxLength)
			throws Exception;

	/**
	 * @param datatypePropertyIndex
	 * @param rdfType
	 * @return
	 * @throws Exception
	 */
	ResultSetCallback getDatatypePropertyValues(int datatypePropertyIndex,
			int rdfType) throws Exception;

	/**
	 * @param type
	 * @param limit
	 * @return
	 * @throws Exception
	 */
	ResultSetCallback getInstancesOfTypes(int type, int limit)
			throws Exception;


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
	int[] getDatatypePropertyByClass(int cluster, double threshold) throws Exception;

	/**
	 * @return
	 */
	URI getUri();
	
	
	void preprocessRdfData(InputStream[] datasets, MediaType rdfMimeType, MediaType fileMimeType, String absoluteBaseURI) throws Exception;
	
	
}
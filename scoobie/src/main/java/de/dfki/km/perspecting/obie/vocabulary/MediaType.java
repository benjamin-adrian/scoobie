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

/**
 * 
 */
package de.dfki.km.perspecting.obie.vocabulary;

/**
 * <ul>
 * <li> RDF/XML application/rdf+xml</li>
 * <li> N-Triples text/plain </li>
 * <li> Turtle application/x-turtle</li>
 * <li> N3 text/rdf+n3 </li>
 * <li> TriX application/trix </li>
 * <li> TriG application/x-trig </li>
 * <li> SPARQL Query Results XML Format application/sparql-results+xml </li>
 * <li> SPARQL Query Results JSON Format application/sparql-results+json </li>
 * <li> binary RDF results table format application/x-binary-rdf-results-table</li>
 * </ul>
 * @author adrian
 * 
 */
public enum MediaType {

	TEXT("text/plain"),
	HTML("text/html"),
	XHTML("application/xhtml+xml"),
	RDF_XML("application/rdf+xml"),
	TURTLE("application/x-turtle"),
	N3("text/rdf+n3"),
	TRIG("application/x-trig"),
	TRIX("application/trix"),
	SPARQL_XML("application/sparql-results+xml"),
	SPARQL_JSON("sparql-results+json"),
	RDF_BINARY("application/x-binary-rdf-results-table"),
	BZIP("application/bzip2"),
	GZIP("application/gzip"),
	ZIP("application/zip");
	
	
	private String value;
	
	private MediaType(String contentType) {
		this.value = contentType;
	}

	public String toString() {
		return value;
	}

	

}

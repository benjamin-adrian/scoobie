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

package de.dfki.km.perspecting.obie.workflow;

import java.io.Reader;

import de.dfki.km.perspecting.obie.connection.KnowledgeBase;
import de.dfki.km.perspecting.obie.model.Document;

/**
 * 
 * A {@link Transducer} is a single processing unit implemented as procedure.
 * 
 * 
 * @author adrian
 * 
 */
public abstract class Transducer {


	/**
	 * Interprets the content of a document by using formal and textual
	 * background knowledge.
	 * 
	 * 
	 * @param document
	 *            the input data
	 * @param kb
	 *            formal background knowledge from an existing knowledge base
	 * @throws Exception
	 */

	public abstract void transduce(Document document, KnowledgeBase kb) throws Exception;

	/**
	 * Compares the tranducer's output result with a corresponding ground truth.
	 * 
	 * @param document
	 *            the input data
	 * @param kb
	 *            formal background knowledge from an existing knowledge base  
	 *           
	 * @param gt
	 *            the correct output
	 * @return A text describing the result of this comparison
	 * @throws Exception 
	 */
	public String compare(Document document, KnowledgeBase kb, Reader gt) throws Exception {
		return "";
	}


}

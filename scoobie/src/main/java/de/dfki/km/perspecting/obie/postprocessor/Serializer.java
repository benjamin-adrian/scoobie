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

import de.dfki.km.perspecting.obie.connection.KnowledgeBase;
import de.dfki.km.perspecting.obie.model.Document;

/**
 * 
 * The {@link Serializer} interface encapsulates different implementations that
 * serialize information extraction results in various formats.
 * 
 * @author adrian
 *
 */
public interface Serializer {

	/**
	 * Returns an extraction result in  a serialized format.
	 * 
	 * @param document The extracted results 
	 * @return A {@link Reader} object that contains a {@link String} representation
	 * of serialized results.
	 * @throws Exception 
	 */
	Reader serialize(Document document, KnowledgeBase kb) throws Exception;
	
}

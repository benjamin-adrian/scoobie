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

import java.sql.ResultSet;

/**
 * The {@link RemoteCursor} interface wraps remote cursors or iterators, such as a
 * {@link ResultSet}. It should be used when the total amount of expected query results
 * would pollute the heap.
 * 
 * @author adrian
 *
 */
public interface RemoteCursor {
	
	/**
	 * Closes the cursor server side.
	 *  
	 * @throws Exception
	 */
	public void close() throws Exception;
	
	/**
	 * Set the current position to the next entry and
	 * return if it contains more entries.
	 * 
	 * @return <code>true</code> if cursor holds more entries and <code>false</code> if not.
	 * @throws Exception
	 */
	public boolean next() throws Exception;
	
	/**
	 * Returns the <code>int</code> value of the current entry at given index.
	 * 
	 * @param index 
	 * @throws Exception
	 */
	public int getInt(int index) throws Exception;
	
	/**
	 * Returns the <code>double</code> value of the current entry at given index.
	 * 
	 * @param index 
	 * @throws Exception
	 */
	public double getDouble(int index) throws Exception;
	
	/**
	 * Returns the <code>String</code> value of the current entry at given index.
	 * 
	 * @param index 
	 * @throws Exception
	 */
	public String getString(int index) throws Exception;
	
}
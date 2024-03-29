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
 * {@link RemoteCursor} implementation base don a {@link ResultSet}.
 * 
 * @author adrian
 *
 */
public class ResultSetCursor implements RemoteCursor {

	private final ResultSet rs;
	
	
	public ResultSetCursor(ResultSet rs) {
		this.rs = rs;
	}
		
	public void close() throws Exception {
		rs.close();
	}


	@Override
	public boolean next() throws Exception {
		return rs.next();
	}


	@Override
	public int getInt(int index) throws Exception {
		return rs.getInt(index);
	}
	
	@Override
	public double getDouble(int index) throws Exception {
		return rs.getDouble(index);
	}


	@Override
	public String getString(int index) throws Exception {
		return rs.getString(index);
	}
		
}

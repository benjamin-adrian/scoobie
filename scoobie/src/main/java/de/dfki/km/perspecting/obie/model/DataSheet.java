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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Map.Entry;

/**
 * 
 * This data sheet summarizes emerging hypotheses raised by extraction tasks.
 * 
 * @author adrian
 * 
 */
public class DataSheet {

	private final Map<String, Map<String, ?>> matrix = new TreeMap<String, Map<String, ?>>();

	private final Set<String> doubleColumns = new HashSet<String>();

	private final List<String> keys = new ArrayList<String>();


	public void createToken(final int start, final int end) {
		add(TokenSequence.TOKEN, start, end);
	}

	public <T> void add(String column, String key, T value) {
		if (!matrix.containsKey(column)) {
			matrix.put(column, new TreeMap<String, T>());
			keys.add(column);
		}

		Map<String, T> row = (Map<String, T>) matrix.get(column);
		row.put(key, value);

		if (value.getClass().equals(Double.class)) {
			doubleColumns.add(column);
		}
	}

	public <T> void add(String column, int key, T value) {
		add(column, Integer.toString(key), value);
	}

	public <T> T get(String column, String key) {
		if (matrix.containsKey(column)) {
			Map<String, T> row = (Map<String, T>) matrix.get(column);
			return row.get(key);
		} else {
			return null;
		}
	}

	public <T> T get(String column, int key) {
		return get(column, Integer.toString(key));
	}

	public Set<Integer> getIntegerKeys(String column) {
		if (matrix.containsKey(column)) {

			TreeSet<Integer> integerSet = new TreeSet<Integer>();

			Map<String, ?> row = (Map<String, ?>) matrix.get(column);
			for (String k : row.keySet()) {
				integerSet.add(Integer.parseInt(k));
			}
			return integerSet;
		} else {
			return new TreeSet<Integer>();
		}
	}

	public TreeSet<String> getKeys(String column) {
		if (matrix.containsKey(column)) {
			Map<String, ?> row = (Map<String, ?>) matrix.get(column);
			return new TreeSet<String>(row.keySet());
		} else {
			return new TreeSet<String>();
		}
	}


	public Set<Entry<String, Integer>> integerEntries(String column) {
		if (matrix.containsKey(column)) {
			Map<String, Integer> row = (Map<String, Integer>) matrix.get(column);
			return row.entrySet();
		} else {
			return new TreeSet<Entry<String, Integer>>();
		}
	}



	public String[] getColumns() {
		return new TreeSet<String>(matrix.keySet()).toArray(new String[matrix
				.keySet().size()]);

	}


}

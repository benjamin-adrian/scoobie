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

import java.util.Map;

public class TIntDoubleTuple implements Map.Entry<Integer, Double>, Comparable<TIntDoubleTuple> {
	int key;
	double value;

	public TIntDoubleTuple(int key, double value) {
		this.key = key;
		this.value = value;
	}

	@Override
	public Integer getKey() {
		return key;
	}

	@Override
	public Double getValue() {
		return value;
	}

	@Override
	public Double setValue(Double value) {
		this.value = value;
		return value;
	}

	@Override
	public int compareTo(TIntDoubleTuple o) {
		if (o.key == key) {
			return (int) Math.signum(o.value - value);
		} else {
			return o.key - key;
		}
	}

	@Override
	public String toString() {
		return String.format("%d=%1.2f ", key, value);
	}

}
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

public class RDFEdge {

	private final long id;
	private static long count = 0;
	private int predicate;
	private double weight = 1.0;

	public RDFEdge(int predicate) {
		this.id = count++;
		this.predicate = predicate;
	}

	public RDFEdge(int predicate, double weight) {
		this.id = count++;
		this.predicate = predicate;
		this.weight = weight;
	}

	public int getPredicate() {
		return predicate;
	}

	public double getWeight() {
		return weight;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof RDFEdge) {
			return equals((RDFEdge) obj);
		}
		return false;
	}

	public boolean equals(RDFEdge obj) {
		return id == obj.id;
	}

	@Override
	public String toString() {
		return Integer.toString(predicate);
	}

}
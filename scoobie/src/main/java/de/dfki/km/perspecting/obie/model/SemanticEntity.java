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

import gnu.trove.TIntDoubleHashMap;
import gnu.trove.TIntDoubleProcedure;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class SemanticEntity {

	private int rdfPropertyIndex = -1;
	
	private int literalValueIndex;

	private int subjectIndex;
	private String subjectURI;

	private String regex;
	
	private TIntDoubleHashMap typeIndexes = new TIntDoubleHashMap();

	private String position;

	public void setRegex(String regex) {
		this.regex = regex;
	}
	
	public String getRegex() {
		return regex;
	}
	
	public void setPosition(String position) {
		this.position = position;
	}

	public String getPosition() {
		return position;
	}

	public int getLiteralValueIndex() {
		return literalValueIndex;
	}

	public int getPropertyIndex() {
		return rdfPropertyIndex;
	}

	public int getSubjectIndex() {
		return subjectIndex;
	}

	public String getSubjectURI() {
		return subjectURI;
	}

	


	public List<TIntDoubleTuple> getTypeIndex() {

		final ArrayList<TIntDoubleTuple> l = new ArrayList<TIntDoubleTuple>();

		typeIndexes.forEachEntry(new TIntDoubleProcedure() {

			@Override
			public boolean execute(int a, double b) {
				l.add(new TIntDoubleTuple(a, b));
				return true;
			}
		});

		Collections.sort(l);

		return l;
	}

	public void setLiteralValueIndex(int literalValueIndex) {
		this.literalValueIndex = literalValueIndex;
	}

	public void setPropertyIndex(int rdfPropertyIndex) {
		this.rdfPropertyIndex = rdfPropertyIndex;
	}

	public void setSubjectIndex(int subjectIndex) {
		this.subjectIndex = subjectIndex;
	}

	public void addTypeIndex(int typeIndex, double probability) {
		typeIndexes.put(typeIndex, probability);
	}

	public void setSubjectURI(String subjectURI) {
		this.subjectURI = subjectURI;
	}

	@Override
	public String toString() {
		return "subject=" + subjectIndex + "; property=" + rdfPropertyIndex
				+ "; type=" + Arrays.toString(typeIndexes.keys());
	}
}

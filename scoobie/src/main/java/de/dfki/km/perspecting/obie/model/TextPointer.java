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

/**
 * 
 * @author adrian
 * 
 */
public class TextPointer implements Comparable<TextPointer>, CharSequence {

	private final int a;
	private final int b;

	private double belief = 1.0;

	private int data = -1;
	private int datatypeProperty = -1;

	private final CharSequence text;

	private String literal;

	public TextPointer(int a, int b, CharSequence text) {
		this.a = a;
		this.b = b;
		this.text = text;

		// log.info("created " + this);
	}

	// public TextPointer(int a, int b, CharSequence text, int data) {
	// this.a = a;
	// this.b = b;
	// this.text = text;
	// this.data = data;
	// // log.info("created " + this + " " + data);
	// }

	public TextPointer(int a, int b, String text, int data, double belief) {
		this.a = a;
		this.b = b;
		this.text = text;
		this.data = data;
		this.belief = belief;
		// log.info("created " + this + " " + data + " " + belief);
	}

	public TextPointer(int a, int b, String text, int data,
			int datatypeProperty, double belief, String literal) {
		
		this.a = a;
		this.b = b;
		this.text = text;
		this.data = data;
		this.belief = belief;
		this.datatypeProperty = datatypeProperty;
		this.literal = literal;
//		log.info("created " + this + " " + data + " " + belief +" " + a + " " + b);
	}
	

	public int getA() {
		return a;
	}

	public int getB() {
		return b;
	}

	public String getLiteral() {
		return literal;
	}
	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {

		if (obj instanceof TextPointer) {
			TextPointer o = ((TextPointer) obj);
			return o.a == a && o.b == b && o.data == data && o.belief == belief;
		}
		return false;
	}

	/**
	 * @return the data
	 */
	public int getLiteralValueIndex() {
		return data;
	}

	public int getDatatypePropertyIndex() {
		return datatypeProperty;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return a;
	}

	/**
	 * Compares the index position of this {@link TextPointer} and
	 * {@link TextPointer} o.
	 * 
	 * @param o
	 * 
	 * @return this.getA() - o.getA();
	 */
	public int compareTo(TextPointer o) {
		if (a - o.a == 0) {
			int c = o.b - b;
			if(c == 0)  return datatypeProperty - o.datatypeProperty;
			else return c;
		} else {
			return a - o.a;
		}
	}

	@Override
	public String toString() {
		if (b > text.length())
			return text.toString();
		else
			return text.subSequence(a, b).toString();
	}

	@Override
	public char charAt(int index) {
		return text.charAt(index);
	}

	@Override
	public int length() {
		return b - a + 1;
	}

	@Override
	public CharSequence subSequence(int start, int end) {
		return text.subSequence(start, end);
	}

	/**
	 * @param belief
	 *            the belief to set
	 */
	public void setBelief(double belief) {
		this.belief = belief;
	}

	/**
	 * @return the belief
	 */
	public double getBelief() {
		return belief;
	}
	
	
}
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

package de.dfki.km.perspecting.obie.transducer.model;

import gnu.trove.TIntHashSet;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.ibm.icu.text.Collator;

import de.dfki.km.perspecting.obie.connection.KnowledgeBase;
import de.dfki.km.perspecting.obie.connection.RemoteCursor;
import de.dfki.km.perspecting.obie.connection.RemoteCursor;
import de.dfki.km.perspecting.obie.model.TextPointer;
import de.dfki.km.perspecting.obie.model.Token;

/**
 * A {@link SuffixArray} is a data structure about text strings that can decide
 * whether a {@link String} is part of a text or not. It can be compared to
 * other SuffixArray instances or {@link BStarCursor} instances. It can be
 * created in O(text.length() * Math.log(text.length())).
 * 
 * @author Benjamin Adrian
 * @author Menna Ghoneim
 * @version 0.1
 */

// TODO improve complexity of creation to linear time O(n + c).

public class SuffixArray {

	private final Logger log = Logger.getLogger(SuffixArray.class.getName());
	private final String text;

	private List<CharSequence> index = new ArrayList<CharSequence>();
	private List<String> index2 = new ArrayList<String>();
	public static int PREFIX_SIZE = 4;
	private final static Pattern p = Pattern
			.compile("[^\\p{L}0-9\\s]+|[\\p{L}0-9]+");

	final ArrayList<Integer> indexes = new ArrayList<Integer>();

	private final TIntHashSet commonPrefixStrings = new TIntHashSet();

	private KnowledgeBase ontology;
	private int maxLength;

	private final static CaseSensitiveComparator prefix_collator = new CaseSensitiveComparator();

	// private final static ArrayLineComparator collator = new
	// ArrayLineComparator();

	/**
	 * Creates a new {@link SuffixArray} about a text {@link String}.
	 * 
	 * @param text
	 *            A text as {@link String}.
	 * @throws Exception
	 */
	public SuffixArray(final String text, KnowledgeBase ontology, int maxLength)
			throws Exception {
		this.maxLength = maxLength;
		this.ontology = ontology;
		prefix_collator.setStrength(Collator.PRIMARY);
		this.text = text;
		create(text);
	}

	public SuffixArray(List<Token> tokens, KnowledgeBase ontology, int maxLength)
			throws Exception {
		this.maxLength = maxLength;
		this.ontology = ontology;
		prefix_collator.setStrength(Collator.PRIMARY);
		if (!tokens.isEmpty())
			this.text = tokens.iterator().next().getTextSource();
		else
			text = "";
		create(tokens);

	}

	/**
	 * This method should be overridden by a more efficient implementation.
	 * 
	 * @param text
	 *            A text as {@link String}.
	 * @throws Exception
	 */
	protected void create(final String text) throws Exception {

		/**
		 * This if condition is added so that if the text is of the structure of
		 * n-gram (from the fusing HWR and SCOOBIE app) it creates the indexes
		 * according to the n-gram nature.
		 * 
		 * It checks if it starts with the separating indexes of the n-gram
		 * virtual document which is unlikely to have a normal text start with.
		 * 
		 * This could be avoided if the prefix array could be created at the
		 * application end and set from the start.
		 * 
		 * @author Menna Ghoneim
		 */
		if (text.startsWith("(##0,0##)")) {
			create(text, true);
			return;
		}

		final Matcher m = p.matcher(text);

		while (m.find()) {
			indexes.add(m.end());
		}
		if (indexes.get(0) != 0) {
			indexes.add(0, -1);
		}
		// indexes.add(text.length() - 1);
		for (int i = indexes.size() - 2; i >= 0; i--) {
			ArrayLine al = new ArrayLine(indexes.get(i) + 1, text.length());
			index.add(al);
		}
		// select * from (values('James „Buster“'), ('Trevor "Hockey"'),
		// ('Trevorcorbin')) AS t(string) order by string
		// Collections.sort(index, collator);
		dbSort();
	}

	/**
	 * This method is done only when the scoobie is used for the application of
	 * fusing HWR with scoobie. It is chosen instead the normal create method in
	 * order to accomodate the nature of the n-gram
	 * 
	 * It could be optimized in order to reduce the processing time.
	 * 
	 * @author Menna Ghoneim
	 */

	protected void create(final String text, boolean b) throws Exception {

		// each ngram starts with a new line
		String[] test = text.split("\n");

		String prefix = "";
		// the prefix of each ngram is taken in the CPS since ngrams anyway
		// start from every word.
		for (int i = 0; i < test.length; i++) {
			// This is the end separator for each ngram, in order to discard it.
			int j = test[i].indexOf("##)");
			if (j > 0)
				test[i] = test[i].substring(j + 3);

			if (PREFIX_SIZE < test[i].length()) {
				prefix = test[i].substring(0, PREFIX_SIZE);
			} else {
				prefix = test[i];
			}
			if (!prefix.equals("")) {
				if (Character.isLetterOrDigit(prefix.charAt(0))) {
					commonPrefixStrings.add(prefix.hashCode());
				}

				ArrayLine al = new ArrayLine(text.indexOf(test[i]), text
						.indexOf(test[i])
						+ test[i].length());
				index.add(al);
			}
		}

		dbSort();

		log.info("Size of suffix array: " + this.index.size());
	}

	protected void dbSort() throws Exception {
		RemoteCursor rs = ontology.dbSort(index, maxLength);
		if (rs != null) {
			while (rs.next()) {
				index2.add(rs.getString(1));
				// System.out.println(rs.getRs().getString(1).replaceAll("\n",
				// " "));
			}

			rs.close();
		}
	}

	public int[] getCommonPrefixStrings() {
		return commonPrefixStrings.toArray();
	}

	public int commonPrefixSize() {
		return commonPrefixStrings.size();
	}

	protected void create(List<Token> tokens) throws Exception {

		/**
		 * This if condition is added so that if the text is of the structure of
		 * n-gram (from the fusing HWR and SCOOBIE app) it creates the indexes
		 * according to the n-gram nature.
		 * 
		 * It checks if it starts with the separating indexes of the n-gram
		 * virtual document which is unlikely to have a normal text start with.
		 * 
		 * This could be avoided if the prefix array could be created at the
		 * application end and set from the start.
		 * 
		 * @author Menna Ghoneim
		 */
		if (text.startsWith("(##0,0##)")) {
			create(text, true);
			return;
		}

		log.info("Creating sorted token list of size " + tokens.size());
		for (Token t : tokens) {
			String prefix;
			if (t.getStart() + PREFIX_SIZE < t.getTextSource().length()) {
				prefix = t.getTextSource().substring(t.getStart(),
						t.getStart() + PREFIX_SIZE);
			} else {
				prefix = t.getTextSource().substring(t.getStart());
			}

			if (Character.isLetterOrDigit(prefix.charAt(0))) {
				// String lc = prefix.toLowerCase();
				commonPrefixStrings.add(prefix.hashCode());
				commonPrefixStrings.add(prefix.toLowerCase().hashCode());
				// commonPrefixStrings.add(lc);
			}

			int min = Math.min(t.getTextSource().length(), t.getStart()
					+ maxLength);

			ArrayLine al = new ArrayLine(t.getStart(), min);
			index.add(al);
			// indexes.add(t.getEnd());
		}
		dbSort();

		log.info("Common lowercase prefixes as String: "
				+ commonPrefixStrings.toString());
		log.info("Size of suffix array: " + this.index.size());
	}

	/**
	 * Returns a list of String indices in text where the String s matches.
	 * 
	 * @param s
	 *            The String to match in text.
	 * @return a collection of indices where s matches with parts of the text.
	 */
	// public Collection<Integer> contains(final String s) {
	//
	// // index position of a match.
	// final int position = Collections.binarySearch(index, s, collator);
	//
	// final ArrayList<Integer> result = new ArrayList<Integer>();
	//
	// if (position > -1) {
	// // search at left position
	// for (int i = position - 1; i >= 0 && index.get(i).compareTo(s) == 0; i--)
	// {
	// result.add(0, index.get(i).start);
	// }
	// // add current position.
	// result.add(index.get(position).start);
	// // search at right position
	// for (int i = position + 1; i < index.size()
	// && index.get(i).compareTo(s) == 0; i++) {
	// result.add(index.get(i).start);
	// }
	// }
	// return result;
	// }

	/**
	 * 
	 * @param s
	 *            The String to partly match in text.
	 * @param size
	 *            minimum size of the matching parts.
	 * @return a collection of indices where s matches at least partly with the
	 *         text.
	 */
	// public Collection<Integer> containsSubstring(final String s, final int
	// size) {
	//
	// final int position = Collections.binarySearch(index, s, collator);
	//
	// final ArrayList<Integer> result = new ArrayList<Integer>();
	//
	// if (position != -1) {
	// for (int i = position - 1; i >= 0
	// && Math.abs(index.get(i).compareTo(s)) > size
	// || index.get(i).compareTo(s) == 0; i--) {
	// result.add(0, index.get(i).start);
	// }
	// result.add(index.get(position).start);
	// for (int i = position + 1; i < index.size()
	// && Math.abs(index.get(i).compareTo(s)) > size
	// || index.get(i).compareTo(s) == 0; i++) {
	// result.add(index.get(i).start);
	// }
	// }
	// return result;
	// }

	// /**
	// * Compares an ordered list of labels behind a {@link BufferedReader} with
	// * this SuffixArray.
	// *
	// * @param array
	// * A {@link BufferedReader} about a file with an ordered list of
	// * strings.
	// * @return a collection of {@link TextPointer} where list content matches
	// at
	// * least partly with the text.
	// */
	// public List<TextPointer> compare(final BufferedReader array)
	// throws IOException {
	// final ArrayList<TextPointer> pairs = new ArrayList<TextPointer>();
	//
	// int indexA = 0;
	// int indexB = 1;
	//
	// ArrayLine lineA;
	//
	// String lineB = array.readLine();
	//
	// while (indexA < this.index.size() && lineB != null) {
	// // System.out.print(lineB + " ");
	// lineA = index.get(indexA);
	// // System.out.println(lineA);
	// int comparison = lineA.compareTo(lineB);
	// System.out.println(comparison);
	// if (comparison == 0) {
	// int end = Math.min(lineA.end, lineA.start + lineB.length());
	// if (this.indexes.contains(end)
	// && end - lineA.start == lineB.length()) {
	// // System.out.println(true);
	// pairs.add(new TextPointer(lineA.start, end, text, 0));
	// }
	// lineB = array.readLine();
	// indexB++;
	// }
	//
	// if (comparison > 0) {
	// lineB = array.readLine();
	// indexB++;
	// }
	//
	// if (comparison < 0) {
	// indexA++;
	// }
	//
	// }
	//
	// log.info("Size of ResultSet: " + indexB);
	//
	// return pairs;
	//
	// }

	/**
	 * Compares content of a {@link BStarCursor} with this SuffixArray.
	 * 
	 * @param array
	 *            A {@link BStarCursor} about an ordered list of strings.
	 * @return a collection of {@link TextPointer} where cursor content matches
	 *         at least partly with the text.
	 * @throws Exception
	 */
	public List<TextPointer> compare(final RemoteCursor rs) throws Exception {

		final ArrayList<TextPointer> pairs = new ArrayList<TextPointer>();

		int indexA = 0;
		int indexB = 1;

		String dbLine = null;
		String saLine;

		boolean stepNext = rs.next();
		int comparison;
		while (indexA < this.index2.size() && stepNext) {
			dbLine = rs.getString(1);
			saLine = index2.get(indexA).toLowerCase(Locale.US);
			//
			// if (saLine.startsWith("barack obama") && dbLine.startsWith("b"))
			// {
			// System.out.println("A " + dbLine);
			// System.out.println("B " + saLine);
			// System.out.println(prefix_collator.compare(saLine, dbLine));
			// }

			// if(lineA.toLowerCase().startsWith("toad") &&
			// lineB.toLowerCase().startsWith("toad")) {
			// System.out.println("A "+ lineB);
			// }

			comparison = prefix_collator.compare(saLine, dbLine);

			if (comparison == 0) {

				int end = Math.min(saLine.length(), dbLine.length());
				// if (this.indexes.contains(end)
				// && end - lineA.start == lineB.length()) {

				Matcher m = Pattern.compile(Pattern.quote(saLine),
						Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE)
						.matcher(text);
				if (m.find()) {
					pairs.add(new TextPointer(m.start(), m.start() + end, text,
							rs.getInt(2), rs.getInt(3), rs.getDouble(4), rs
									.getString(5)));
				} else {
					throw new Exception("Could not find: \"" + saLine
							+ "\" in \n" + text);
				}

				// }

				int i = 1;
				String tmpLineA = null;
				int tmpComparison = -1;

				if (indexA + i < index2.size()) {
					do {
						tmpLineA = index2.get(indexA + i);
						tmpComparison = prefix_collator.compare(tmpLineA,
								dbLine);

						if (tmpComparison == 0) {

							end = Math.min(tmpLineA.length(), dbLine.length());
							m = Pattern.compile(
									Pattern.quote(tmpLineA),
									Pattern.CASE_INSENSITIVE
											| Pattern.UNICODE_CASE).matcher(
									text);
							if (m.find()) {
								pairs.add(new TextPointer(m.start(), m.start()
										+ end, text, rs.getInt(2),
										rs.getInt(3), rs.getDouble(4), rs
												.getString(5)));
							} else {
								throw new Exception("Could not find: \""
										+ saLine + "\" in \n" + text);
							}
						}
						i++;

					} while (tmpComparison == 0 && indexA + i < index.size());
				}
				stepNext = rs.next();
				indexB++;
			}

			if (comparison > 0) {
				stepNext = rs.next();
				indexB++;
			}

			if (comparison < 0) {
				indexA++;
			}

		}

		return pairs;
	}

	public boolean startsWith(String value, String prefix) {
		char ta[] = value.toCharArray();
		int to = 0;
		char pa[] = prefix.toCharArray();
		int po = 0;
		int pc = prefix.length();

		while (--pc >= 0) {
			if (ta[to] != pa[po] && (Math.abs(ta[to] - pa[po])) != 32) {
				return false;
			}
			to++;
			po++;
		}
		return true;
	}

	@Override
	public String toString() {

		final StringBuilder buffer = new StringBuilder();

		for (CharSequence line : index) {
			buffer.append(line.toString());
			buffer.append('\n');
		}

		return buffer.toString();
	}

	private final class ArrayLine implements Comparable<Object>, CharSequence {

		final int start;
		final int end;

		ArrayLine(final int start, final int end) {
			this.start = start;
			this.end = end;
		}

		@Override
		public int compareTo(final Object o) {
			if (o instanceof ArrayLine) {
				return this.compareTo(o.toString());
			} else {
				return this.compareTo((String) o);
			}
		}

		@Override
		public String toString() {
			return text.subSequence(start, end).toString();
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see java.lang.CharSequence#charAt(int)
		 */
		@Override
		public char charAt(int index) {
			return text.charAt(start + index);
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see java.lang.CharSequence#length()
		 */
		@Override
		public int length() {
			return end - start;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see java.lang.CharSequence#subSequence(int, int)
		 */
		@Override
		public CharSequence subSequence(int start, int end) {
			return text.substring(this.start + start, this.start + end);
		}
	}

	public static class CaseSensitiveComparator implements Comparator<String>,
			java.io.Serializable {
		// private static final String WHITESPACE = " ";

		// use serialVersionUID from JDK 1.2.2 for interoperability
		private static final long serialVersionUID = 8575799808933029326L;

		// Pattern p = Pattern.compile("[^\\p{L}0-9]");

		public int compare(String sa, String db) {

			int min = Math.min(sa.length(), db.length());

			if (sa.length() < db.length()) {
				return sa.compareTo(db.substring(0, min));
			} else {
				return sa.substring(0, min).compareTo(db.substring(0, min));
			}

		}

		public void setStrength(int strategy) {

		}
	}

	public void clean() {

		this.commonPrefixStrings.clear();
		this.index.clear();
		this.index2.clear();
		this.indexes.clear();

	}

	// protected static class ArrayLineComparator implements
	// Comparator<ArrayLine>, java.io.Serializable {
	// // private static final String WHITESPACE = " ";
	//
	// // use serialVersionUID from JDK 1.2.2 for interoperability
	// private static final long serialVersionUID = 8575799808933029326L;
	// Pattern p = Pattern.compile("[^\\p{L}0-9]");
	// final Collator coll = Collator.getInstance(Locale.US);
	//
	// public ArrayLineComparator() {
	// coll.setStrength(Collator.PRIMARY);
	// }
	//
	// public int compare(ArrayLine s1, ArrayLine s2) {
	//			
	// String a = p.matcher(s1).replaceAll("");
	// String b = p.matcher(s2).replaceAll("");
	//			
	// int min = Math.min(a.length(), b.length());
	//			
	// // return coll.compare(s1.subSequence(0, min), s2.subSequence(0, min));
	// return coll.compare(a.substring(0, min), b.substring(0, min));
	//			
	// // return coll.compare(s1.subSequence(0, min), s2.subSequence(0, min));
	// }
	//
	// public void setStrength(int strategy) {
	//
	// }
	// }
}

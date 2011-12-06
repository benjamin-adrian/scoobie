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

package de.dfki.km.perspecting.obie.corpus;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.io.Writer;
import java.net.URI;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.apache.commons.io.FileUtils;

import de.dfki.km.perspecting.obie.model.DocumentProcedure;
import de.dfki.km.perspecting.obie.vocabulary.MediaType;

/**
 * 
 * A standard labeled text corpus contains a line-based sequences of words. Each
 * word is followed by a set of features. Each feature separated by simple
 * space. In front of each word is a label that classifies it. Two newlines in
 * sequence separate sentences. e.g.,
 * 
 * <pre>
 * LABEL WORD FEATURE FEATURE ... FEATURE
 * LABEL WORD FEATURE FEATURE ... FEATURE
 * 
 * LABEL WORD FEATURE FEATURE ... FEATURE
 * LABEL WORD FEATURE FEATURE ... FEATURE
 * </pre>
 * 
 * @author adrian
 * 
 */
public class LabeledTextCorpus extends TextCorpus {

	private static final String PREPOSITION = "PRP";
	private static final String SUFFIX = "s:";
	private static final String COLON = ",";
	private static final String IN = "i:";
	private static final String POST = "p:";
	private static final String PRE = "a:";

	final static int WORD = 0;
	final static int POS = 1;
	final static int PHRASE = 2;
	final static int LABEL = 3;

	final static String OUTSIDE_ANY_LABEL = "O";

	private static final String NEWLINE = "\n";
	private static final String SPACE = " ";

	private static String CAPS = "[\\p{Lu}]";
	private static String ALPHA = "[\\p{Lu}\\p{Ll}]";
	private static String PUNT = "[,\\.;:?!()]";
	private static String QUOTE = "[\"`']";

	// private static String CAPSNUM = "[\\p{Lu}\\p{Nd}]";
	// private static String ALPHANUM = "[\\p{Lu}\\p{Ll}\\p{Nd}]";
	// private final static Pattern ALLCAPS = Pattern.compile(CAPS + "+");
	// private final static Pattern CONTAINSDIGITS =
	// Pattern.compile(".*[0-9].*");
	// private final static Pattern ACRO = Pattern
	// .compile("[A-Z][A-Z\\.]*\\.[A-Z\\.]*");
	// private static Pattern ALPHNUMERIC = Pattern.compile("[A-Za-z0-9]+");

	private final static Pattern MIXEDCAPS = Pattern
			.compile("[A-Z][a-z]+[A-Z][A-Za-z]*");
	private final static Pattern ALLDIGITS = Pattern.compile("[0-9]+");
	private final static Pattern NUMERICAL = Pattern
			.compile("[-0-9]+[\\.,]+[0-9\\.,]+");
	private final static Pattern ROMAN = Pattern
			.compile("[ivxdlcm]+|[IVXDLCM]+");
	private final static Pattern MULTIDOTS = Pattern.compile("\\.\\.+");
	private final static Pattern ABBR = Pattern.compile(ALPHA + ALPHA + "+\\.");

	private final static Pattern LONELYINITIAL = Pattern.compile(CAPS + "\\.");
	private final static Pattern SINGLECHAR = Pattern.compile(ALPHA);
	private final static Pattern CAPLETTER = Pattern.compile("[A-Z]");
	private final static Pattern PUNC = Pattern.compile(PUNT);
	private final static Pattern QUOTES = Pattern.compile(QUOTE + ALPHA + "?");
	private final static Pattern ENUM = Pattern.compile("[0-9]+[" + PUNT
			+ "a-z]+");
	private final static Pattern NUMRANGE = Pattern.compile("[0-9]+-[0-9]+");

	private final static Pattern DATE = Pattern
			.compile("[0-9]+[\\p{Punct}[0-9]+]+");

	private File labelFolder;
	private MediaType labelFileMediaType;

	public LabeledTextCorpus(File labelFolder, MediaType labelFileMediaType,
			TextCorpus corpus) throws Exception {
		super(corpus.getCorpus(), corpus.getCorpusFileMediaType(),
				corpus.corpusMediaType, corpus.language);
		this.labelFolder = labelFolder;
		this.labelFileMediaType = labelFileMediaType;
	}

	public Reader getGroundTruth(final URI uri) throws Exception {
		if (labelFileMediaType == MediaType.DIRECTORY) {
			return new StringReader(FileUtils.readFileToString(new File(uri)));
		} else if (labelFileMediaType == MediaType.ZIP) {
			ZipFile zipFile = new ZipFile(labelFolder);
			String[] entryName = uri.toURL().getFile().split("/");
			ZipEntry entry = zipFile.getEntry(URLDecoder.decode(
					entryName[entryName.length - 1], "utf-8"));

			if (entry != null) {
				log.info("found labels for: " + uri.toString());
			} else {
				throw new Exception("did not found labels for: " + uri.toString());
			}
			return new InputStreamReader(zipFile.getInputStream(entry));
		} else {
			throw new Exception("Unsupported media format for labels: "
					+ labelFileMediaType + ". "
					+ "Please use zip or plain directories instead.");
		}
	}

	/**
	 * This method is a hook for inserting a label extraction from different
	 * label files.
	 */
	protected Reader extractLabels(Reader in) throws Exception {
		return in;
	}

	public Reader toFeatureFormat(File out, final int[] ngramsize,
			final boolean useContext, final boolean useContent,
			final boolean useRegex, final double typeProportion,
			final int windowsize, final String... postags) throws Exception {
		final BufferedWriter writer = new BufferedWriter(new FileWriter(out));

		this.forEach(new DocumentProcedure<String>() {
			@Override
			public String process(Reader doc, URI uri) throws Exception {

				BufferedReader reader = new BufferedReader(doc);

				List<List<String[]>> sentences = new ArrayList<List<String[]>>();
				List<String[]> _sentence = new ArrayList<String[]>();

				for (String line = reader.readLine(); line != null; line = reader
						.readLine()) {
					if (line.length() == 0 && !_sentence.isEmpty()) {
						sentences.add(_sentence);
						_sentence = new ArrayList<String[]>();
					} else {
						_sentence.add(line.split(" "));
					}
				}

				for (int sentIndex = 0; sentIndex < sentences.size(); sentIndex++) {
					List<String[]> sentence = sentences.get(sentIndex);

					List<Integer> labelIndexes = new ArrayList<Integer>();

					for (int wordIndex = 0; wordIndex < sentence.size(); wordIndex++) {
						String label;
						if (sentence.get(wordIndex).length > 1) {
							label = sentence.get(wordIndex)[LABEL];
							if (label.equals(OUTSIDE_ANY_LABEL)) {
								if (!labelIndexes.isEmpty()) {
									List<String> buffer = extractFeatures(
											labelIndexes, sentence, ngramsize,
											useContext, useContent, useRegex,
											typeProportion, windowsize, postags);

									boolean cont = true;
									int nextSentence = sentIndex + 1;
									while (cont
											&& nextSentence < sentences.size()) {

										String[] wordFeatures = sentences.get(
												nextSentence).get(0);
										if (wordFeatures[POS]
												.startsWith(PREPOSITION)) {
											// System.out.println("followed coreference");

											buffer.addAll(extractFeatures(
													labelIndexes, sentence,
													ngramsize, useContext,
													useContent, useRegex,
													typeProportion, windowsize,
													postags));

											nextSentence++;

										} else {
											cont = false;
										}
									}

									if (!labelIndexes.isEmpty()) {
										String ann = sentence.get(labelIndexes
												.get(0))[LABEL];
										serializeExample(writer, ann, uri + "_"
												+ sentIndex, buffer);
									}
									labelIndexes.clear();
								}
							} else {
								labelIndexes.add(wordIndex);
							}
						}
					}

				}

				return null;
			}
		});

		writer.close();
		return new FileReader(out);
	}

	/**
	 * Writes a single example to a corpus file.
	 * 
	 * @param corpusWriter
	 * @param exampleLabel
	 * @param exampleName
	 * @param exampleData
	 * @throws IOException
	 */
	public static void serializeExample(Writer corpusWriter,
			String exampleLabel, String exampleName, List<String> exampleData)
			throws IOException {
		if (!exampleData.isEmpty()) {

			corpusWriter.append(exampleName);
			corpusWriter.append(SPACE);
			corpusWriter.append(exampleLabel);
			for (String ft : exampleData) {

				corpusWriter.append(SPACE);
				corpusWriter.append(ft);
			}
			corpusWriter.append(NEWLINE);
		}
	}

	/**
	 * This method extracts features from sentences that describe the type of a
	 * given phrase.
	 * 
	 * @param sentence
	 * @param phrase
	 * @return
	 */
	public static List<String> extractFeatures(List<Integer> labelIndexes,
			List<String[]> sentence, int[] nGramSizes, boolean useContext,
			boolean useContent, boolean useRegex, double typeProp,
			int windowsize, String... postags) {

		final List<String> text = new ArrayList<String>();

		final List<String> prefixes = new ArrayList<String>();
		final List<String> infixes = new ArrayList<String>();
		final List<String> postfixes = new ArrayList<String>();

		final Set<String> matchPos = new HashSet<String>(Arrays.asList(postags));

		int lower = labelIndexes.get(0);
		int upper = labelIndexes.get(labelIndexes.size() - 1);

		for (int wordPosInSen = 0; wordPosInSen < sentence.size(); wordPosInSen++) {

			String[] word = sentence.get(wordPosInSen);

			if (wordPosInSen < lower) {
				if (wordPosInSen - lower > -windowsize) {
					prefixes.addAll(scanWordContent(typeProp, matchPos, word,
							useRegex));
				}
			} else if (wordPosInSen >= lower && wordPosInSen <= upper) {
				infixes.addAll(scanWordSyntax(word, useRegex, true));
			} else if (wordPosInSen > upper) {
				if (wordPosInSen - upper < windowsize) {
					postfixes.addAll(scanWordContent(typeProp, matchPos, word,
							useRegex));

				}
			}
		}
		if (useContext) {
			for (int n : nGramSizes) {
				text.addAll(calculateNgrams(n, prefixes, PRE));
				text.addAll(calculateNgrams(n, postfixes, POST));
			}
		}
		if (useContent) {
			text.addAll(calculateNgrams(1, infixes, IN));
		}
		return text;
	}

	/**
	 * Geerates a confusion matrix
	 * 
	 * @param _corpus
	 * @return
	 */
	public Reader compare(LabeledTextCorpus _corpus) {
		return null;
	}

	public static Collection<String> scanWordContent(double probUseType,
			Set<String> matchPos, String[] wordFeatureVector, boolean useRegex) {

		Collection<String> returnValues = new HashSet<String>();

		if (wordFeatureVector.length == 4) {
			if (wordFeatureVector[LABEL].equals(OUTSIDE_ANY_LABEL)) {
				if (wordFeatureVector[POS].length() > 1
						&& matchPos.contains(wordFeatureVector[POS].substring(
								0, 2))) {
					returnValues.addAll(scanWordSyntax(wordFeatureVector,
							useRegex, false));
				} else {
					if (wordFeatureVector[LABEL].length() > 1) {
						returnValues.addAll(scanWordSyntax(wordFeatureVector,
								useRegex, false));
					}
				}
			} else {
				double d = new Random().nextDouble();
				if (d <= probUseType) {
					returnValues.add(wordFeatureVector[LABEL]);
				} else {
					if (wordFeatureVector[LABEL].length() > 1) {
						returnValues.addAll(scanWordSyntax(wordFeatureVector,
								useRegex, false));
					}
				}
			}
		}
		return returnValues;
	}

	public static List<String> calculateNgrams(int nGramSize,
			List<String> sequence, String before) {
		List<String> text = new ArrayList<String>();
		int newNOfNGrams = Math.min(sequence.size(), nGramSize);
		if (newNOfNGrams > 0) {
			for (int i = newNOfNGrams; i <= sequence.size(); i++) {
				StringBuilder b = new StringBuilder();
				List<String> l = sequence.subList(i - newNOfNGrams, i);
				for (int j = 0; j < l.size(); j++) {
					b.append(l.get(j));
					if (j < l.size() - 1) {
						b.append(COLON);
					}
				}
				text.add(before + b);
			}
		}
		return text;
	}

	public static Collection<String> scanWordSyntax(String[] wordFeatureVector,
			boolean useRegex, boolean useSuffix) {

		String token = wordFeatureVector[WORD];

		boolean usedRegex = false;

		Collection<String> tokens = new HashSet<String>();

		if (useRegex) {
			if (MIXEDCAPS.matcher(wordFeatureVector[WORD]).matches()) {
				token = "MIXEDCAPS";
				usedRegex = true;
			}

			if (ALLDIGITS.matcher(wordFeatureVector[WORD]).matches()) {
				token = "ALLDIGITS";
				usedRegex = true;
			}

			if (NUMERICAL.matcher(wordFeatureVector[WORD]).matches()) {
				token = "NUMERICAL";
				usedRegex = true;
			}

			if (DATE.matcher(wordFeatureVector[WORD]).matches()) {
				token = "DATE";
				usedRegex = true;
			}

			if (ROMAN.matcher(wordFeatureVector[WORD]).matches()) {
				token = "ROMAN";
				usedRegex = true;
			}

			if (MULTIDOTS.matcher(wordFeatureVector[WORD]).matches()) {
				token = "MULTIDOTS";
				usedRegex = true;

			}

			if (LONELYINITIAL.matcher(wordFeatureVector[WORD]).matches()) {
				token = "LONELYINITIAL";
				usedRegex = true;
			}

			if (ABBR.matcher(wordFeatureVector[WORD]).matches()) {
				token = "ABBR";
				usedRegex = true;
			}

			if (SINGLECHAR.matcher(wordFeatureVector[WORD]).matches()) {
				token = "SINGLECHARALLCAPS";
				usedRegex = true;
			}

			if (CAPLETTER.matcher(wordFeatureVector[WORD]).matches()) {
				token = "CAPLETTER";
				usedRegex = true;
			}

			if (PUNC.matcher(wordFeatureVector[WORD]).matches()) {
				token = "PUNC";
				usedRegex = true;
			}

			if (QUOTES.matcher(wordFeatureVector[WORD]).matches()) {
				token = "QUOTES";
				usedRegex = true;
			}

			if (ENUM.matcher(wordFeatureVector[WORD]).matches()) {
				token = "ENUM";
				usedRegex = true;
			}

			if (NUMRANGE.matcher(wordFeatureVector[WORD]).matches()) {
				token = "NUMRANGE";
				usedRegex = true;
			}
		}

		if (!usedRegex && token.length() > 3 && useSuffix) {
			tokens.add(SUFFIX
					+ token.substring(token.length() - 3, token.length()));
		}
		tokens.add(token);

		return tokens;
	}

}

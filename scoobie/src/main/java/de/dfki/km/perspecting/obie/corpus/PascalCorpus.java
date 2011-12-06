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

public class PascalCorpus {

//
//	public PascalCorpus(File corpusDir) {
//		super(corpusDir);
//	}
//
//	private static final String SUFFIX = ".cl";
//	private static final String DOCSTART_X_O_O = "-DOCSTART- -X-";
//
//	@Test
//	public void transformPascalToScoobie() throws Exception {
//		String path = "/media/data/pascal/train/gate/";
//		String outPath = "src/test/resources/de/dfki/km/perspecting/obie/dixi/phd/pascal/pascal.train";
//
//		File out = new File(outPath);
//		BufferedWriter writer = new BufferedWriter(new FileWriter(out));
//
//		System.out.println(path);
//		Iterator<File> i = FileUtils.iterateFiles(new File(path), new String[] {
//				"key", "txt" }, true);
//
//		Map<String, Integer> labels = new TreeMap<String, Integer>();
//
//		while (i.hasNext()) {
//			File f = i.next();
//			StringBuffer b = new StringBuffer();
//			System.out.println(f);
//			SAXBuilder builder = new SAXBuilder();
//			Document doc = builder.build(new FileInputStream(f));
//
//			Iterator<Content> iter = (Iterator<Content>) doc.getDescendants();
//
//			String label = "O";
//			String pos = "";
//			String text = "";
//
//			while (iter.hasNext()) {
//
//				Content c = iter.next();
//				if (c instanceof Element) {
//					Element e = (Element) c;
//
//					if (e.getName().equalsIgnoreCase("token")) {
//						text = e.getAttributeValue("string");
//						b.append(text);
//						b.append("\t");
//						pos = e.getAttributeValue("category");
//						b.append(pos);
//						b.append("\t");
//
//						if (!e.getParentElement().getName().equals(
//								"GateApiText")) {
//
//							String oldLabel = label;
//
//							if (e.getParentElement().getParentElement() != null
//									&& !e.getParentElement().getParentElement()
//											.getName().equals("GateApiText")) {
//								label = e.getParentElement().getParentElement()
//										.getName();
//							} else {
//								label = e.getParentElement().getName();
//							}
//
//							if (oldLabel.endsWith(label)) {
//								label = "I-" + label;
//							} else {
//								label = "B-" + label;
//							}
//
//							Integer count = labels.get(label);
//							if (count == null) {
//								count = 0;
//							}
//							labels.put(label, count + 1);
//						} else {
//							label = "O";
//						}
//
//						if (label.equals("O") && !pos.startsWith("NN"))
//							b.append("O\t" + label + "\n");
//						else
//							b.append("I-NP\t" + label + "\n");
//
//						if (text.equals(".") && label.equals("O")) {
//							b.append("\n");
//						}
//
//					} else if (e.getName().equalsIgnoreCase("spacetoken")
//							&& e.getAttributeValue("kind").equals("control")) {
//						// b.append("\n");
//					}
//
//				}
//			}
//
//			writer.append(DOCSTART_X_O_O);
//			writer.newLine();
//			writer.append(b.toString());
//			writer.newLine();
//
//		}
//		writer.close();
//		for (String k : labels.keySet()) {
//			System.out.println(k + "\t" + labels.get(k));
//		}
//	}
//
//	public String pascalToScoobieTest(String corpusPath, String unsureClass,
//			int[] ngramsize, double typeProportion, int windowSize,
//			boolean useContext, boolean useContent, boolean useRegex,
//			Map<String, String> keys, String... postags) throws IOException {
//
//		BufferedReader breader = new BufferedReader(new FileReader(new File(
//				corpusPath)));
//
//		FileWriter fw = new FileWriter(corpusPath + SUFFIX);
//		BufferedWriter corpusWriter = new BufferedWriter(fw);
//		List<String> docLines = new ArrayList<String>();
//
//		for (String line = breader.readLine(); line != null; line = breader
//				.readLine()) {
//
//			if (line.startsWith(DOCSTART_X_O_O)) {
//				if (!docLines.isEmpty()) {
//
//					List<List<List<String>>> document = new ArrayList<List<List<String>>>();
//					parsePascalDocument(
//							docLines, document, unsureClass, keys);
//					extractFeatures(document, corpusWriter, ngramsize,
//							useContext, useContent, useRegex, typeProportion,
//							windowSize, postags);
//					docLines.clear();
//				}
//			} else {
//				docLines.add(line);
//			}
//		}
//		ConllCorpus.serializeExample(corpusWriter, unsureClass, "unkown",
//				Arrays.asList(new String[] { "none, none, none" }));
//		corpusWriter.close();
//		fw.close();
//		breader.close();
//		return corpusPath + SUFFIX;
//	}
//
//	public String pascalToScoobieTrain(String corpusPath, String unsureClass,
//			int[] ngramsize, double typeProportion, int windowSize,
//			boolean useContext, boolean useContent, boolean useRegex,
//			Map<String, String> keys, String... postags) throws IOException {
//
//		BufferedReader breader = new BufferedReader(new FileReader(new File(
//				corpusPath)));
//
//		FileWriter fw = new FileWriter(corpusPath + SUFFIX);
//		BufferedWriter corpusWriter = new BufferedWriter(fw);
//		List<String> docLines = new ArrayList<String>();
//
//		for (String line = breader.readLine(); line != null; line = breader
//				.readLine()) {
//
//			if (line.startsWith(DOCSTART_X_O_O)) {
//				if (!docLines.isEmpty()) {
//
//					List<List<List<String>>> document = new ArrayList<List<List<String>>>();
//					parsePascalDocument(
//							docLines, document, null, keys);
//					extractFeatures(document, corpusWriter, ngramsize,
//							useContext, useContent, useRegex, typeProportion,
//							windowSize, postags);
//					docLines.clear();
//				}
//			} else {
//				docLines.add(line);
//			}
//		}
//		ConllCorpus.serializeExample(corpusWriter, unsureClass, "unkown",
//				Arrays.asList(new String[] { "none, none, none" }));
//		corpusWriter.close();
//		fw.close();
//		breader.close();
//		return corpusPath + SUFFIX;
//	}
//
//	private void parsePascalDocument(List<String> docLines,
//			List<List<List<String>>> sentences, String unsureClass,
//			Map<String, String> keys) {
//
//		ArrayList<List<String>> sentence = new ArrayList<List<String>>();
//
//		for (String line : docLines) {
//			if (line.isEmpty()) {
//				if (!sentence.isEmpty()) {
//					sentences.add(sentence);
//					sentence = new ArrayList<List<String>>();
//				}
//			} else {
//				String[] tokens = line.split("\t");
//
//				if (tokens.length == 4) {
//					if (!tokens[3].equals("O")
//							&& keys.containsKey(tokens[3].substring(2))) {
//						tokens[3] = keys.get(tokens[3].substring(2));
//						sentence.add(Arrays.asList(tokens));
//					} else {
//						sentence.add(Arrays.asList(tokens));
//					}
//				}
//			}
//		}
//	}
//
//	int iCount = 0;
//
//	private void extractFeatures(List<List<List<String>>> document,
//			BufferedWriter corpusWriter, int[] ngramsize, boolean useContext,
//			boolean useContent, boolean useRegex, double typeProportion,
//			int windowSize, String... postags) throws IOException {
//
//		for (int sentIndex = 0; sentIndex < document.size(); sentIndex++) {
//			List<List<String>> sentence = document.get(sentIndex);
//
//			List<Integer> labelIndexes = new ArrayList<Integer>();
//
//			for (int wordIndex = 0; wordIndex < sentence.size(); wordIndex++) {
//				String label = sentence.get(wordIndex).get(
//						sentence.get(wordIndex).size() - 1);
//				if (label.equals("O")) {
//					if (!labelIndexes.isEmpty()) {
//						List<String> buffer = new ArrayList<String>();
//						ConllCorpus.parseSentences(ngramsize, labelIndexes,
//								buffer, sentence, useContext, useContent,
//								useRegex, typeProportion, 4, postags);
//
//						boolean cont = true;
//						int nextSentence = sentIndex + 1;
//						while (cont && nextSentence < document.size()) {
//
//							List<String> wordFeatures = document.get(
//									nextSentence).get(0);
//							if (wordFeatures.get(1).startsWith("PRP")) {
////								System.out.println("followed coreference");
//								ConllCorpus.parseSentences(ngramsize, Arrays
//										.asList(new Integer[] { 0 }), buffer,
//										document.get(nextSentence), useContext,
//										useContent, useRegex, typeProportion,
//										windowSize, postags);
//								nextSentence++;
//
//							} else {
//								cont = false;
//							}
//						}
//
//						if (!labelIndexes.isEmpty()) {
//							String ann = sentence.get(labelIndexes.get(0))
//									.get(
//											sentence.get(labelIndexes.get(0))
//													.size() - 1).substring(2);
//							if (!ann.endsWith("MISC")) {
////								System.out.println(ann);
//								ConllCorpus.serializeExample(corpusWriter, ann,
//										"" + (iCount++), buffer);
//							}
//						}
//						labelIndexes.clear();
//					}
//				} else {
//					labelIndexes.add(wordIndex);
//				}
//			}
//
//		}
//	}

}

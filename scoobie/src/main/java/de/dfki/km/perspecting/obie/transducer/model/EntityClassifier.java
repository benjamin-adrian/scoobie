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

import java.io.File;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import cc.mallet.classify.Classification;
import cc.mallet.classify.MaxEnt;
import cc.mallet.classify.MaxEntTrainer;
import cc.mallet.classify.Trial;
import cc.mallet.classify.evaluate.ConfusionMatrix;
import cc.mallet.pipe.CharSequence2TokenSequence;
import cc.mallet.pipe.FeatureCountPipe;
import cc.mallet.pipe.FeatureSequence2FeatureVector;
import cc.mallet.pipe.Pipe;
import cc.mallet.pipe.SerialPipes;
import cc.mallet.pipe.Target2Label;
import cc.mallet.pipe.TokenSequence2FeatureSequence;
import cc.mallet.pipe.TokenSequenceLowercase;
import cc.mallet.pipe.TokenSequenceRemoveStopwords;
import cc.mallet.pipe.iterator.CsvIterator;
import cc.mallet.types.Alphabet;
import cc.mallet.types.InstanceList;
import cc.mallet.types.LabelAlphabet;
import cc.mallet.types.RankedFeatureVector;

/**
 * 
 * @author adrian
 * 
 */
public class EntityClassifier {

	private final static Logger log = Logger.getLogger(EntityClassifier.class
			.getName());

	private MaxEnt classifier;

	public EntityClassifier(MaxEnt classifier) {
		this.classifier = classifier;
	}

	/**
	 * 
	 * @param input
	 * @param commonWords
	 * @param prunedWords
	 * @param countStoppWords
	 * @param minimalCount
	 * @throws Exception
	 */
	public void generateStoppwordLists(Reader input, File commonWords,
			File prunedWords, int countStoppWords, int minimalCount)
			throws Exception {
		Pipe pipe = buildFeatureSequencePipe();

		InstanceList data = new InstanceList(pipe);
		data.addThruPipe(new CsvIterator(input, Pattern
				.compile("^(\\S*)[\\s,]*(\\S*)[\\s,]*(.*)$"), 3, 2, 1));

		FeatureCountPipe fc = new FeatureCountPipe(data.getDataAlphabet(), data
				.getTargetAlphabet());
		new InstanceList(fc).addThruPipe(data.iterator());

		fc.writeCommonWords(commonWords, countStoppWords);
		fc.writePrunedWords(prunedWords, minimalCount);
		input.close();
	}

	/**
	 * 
	 * @param input
	 * @param commonWords
	 * @param prunedWords
	 * @return
	 * @throws Exception
	 */
	private InstanceList createInstanceList(Reader input, File commonWords,
			File prunedWords, Pipe pipe) throws Exception {
		InstanceList data = new InstanceList(pipe);
		data.addThruPipe(new CsvIterator(input, Pattern
				.compile("^(\\S*)[\\s,]*(\\S*)[\\s,]*(.*)$"), 3, 2, 1));
//
		log.info("Read " + data.size() + " instances");
		log.info("Feature Count: " + data.getDataAlphabet().size());

		return data;
	}
	
	
	public List<Classification> test(String example) throws Exception {
		InstanceList testing = createInstanceList(
				new StringReader(example), null, null, classifier
						.getInstancePipe());

		return classifier.classify(testing);

	}

	public MaxEnt train(Reader trainingCorpus, File commonWords,
			File prunedWords) throws Exception {

		InstanceList training = createInstanceList(trainingCorpus, commonWords,
				prunedWords, buildPipe(commonWords, prunedWords));

		this.classifier = new MaxEntTrainer().train(training);
		trainingCorpus.close();
		return this.classifier;
	}

	public Map<String, Double[]> evaluate(Reader trainingCorpus,
			File commonWords, File prunedWords, Reader testCorpus,
			boolean crossValidation, double proportion) throws Exception {

		Pipe pipe = buildPipe(commonWords, prunedWords);

		InstanceList training = createInstanceList(trainingCorpus, commonWords,
				prunedWords, pipe);

		InstanceList testing = null;

		if (testCorpus != null) {
			testing = createInstanceList(testCorpus, null, null, pipe);
			testCorpus.close();
		}

		HashMap<String, Double[]> map = new HashMap<String, Double[]>();

		if (testing != null) {
			classifier = evaluateWithTestData(training, testing, map,
					proportion);
		} else if (crossValidation) {
			classifier = evaluateCrossValidation(training, map);
		} else {
			classifier = evaluateWithTestData(training, training, map,
					proportion);
		}

		return map;
	}

	private MaxEnt evaluateWithTestData(InstanceList training,
			InstanceList testing, HashMap<String, Double[]> map,
			double proportion) {

		InstanceList list = training.split(new double[] { proportion,
				1.0 - proportion })[0];
		log.info("Read " + list.size() + " instances");
		log.info("Feature Count: " + list.getDataAlphabet().size());
		classifier = new MaxEntTrainer().train(list);

		Trial t = new Trial(classifier, testing);
		ConfusionMatrix m = new ConfusionMatrix(t);
		log.info(m.toString());
		for (Object s : classifier.getLabelAlphabet().toArray()) {

			Double[] d = map.get(s);
			if (d == null) {
				d = new Double[4];
				map.put(s.toString(), d);
				d[0] = 0.0;
				d[1] = 0.0;
				d[2] = 0.0;
				d[3] = 0.0;

			}

			d[0] += classifier.getPrecision(testing, s);
			d[1] += classifier.getRecall(testing, s);
			d[2] += classifier.getF1(testing, s);
			d[3] += classifier.getAccuracy(testing);
		}

		return classifier;
	}

	private MaxEnt evaluateCrossValidation(InstanceList training,
			HashMap<String, Double[]> map) {
		int k = 10;
		cc.mallet.types.InstanceList.CrossValidationIterator iter = training
				.crossValidationIterator(k);

		while (iter.hasNext()) {
			InstanceList[] lists = iter.next();
			log.info("Cross Training on " + lists[0].size()
					+ " instances");

			classifier = new MaxEntTrainer().train(lists[0]);

			log.info("Cross Testing on " + lists[1].size()
					+ " instances");

			for (Object s : classifier.getLabelAlphabet().toArray()) {

				Double[] d = map.get(s);
				if (d == null) {
					d = new Double[4];
					map.put(s.toString(), d);
					d[0] = 0.0;
					d[1] = 0.0;
					d[2] = 0.0;
				}

				d[0] += classifier.getPrecision(lists[1], s) / k;
				d[1] += classifier.getRecall(lists[1], s) / k;
				d[2] += classifier.getF1(lists[1], s) / k;
			}
		}
		return classifier;
	}

	/**
	 * 
	 * @param out
	 * @param k
	 */
	public void printRank(PrintWriter out, int k) {

		final Alphabet dict = classifier.getAlphabet();
		final LabelAlphabet labelDict = classifier.getLabelAlphabet();

		int numFeatures = dict.size() + 1;
		int numLabels = labelDict.size();
		// Include the feature weights according to each label
		RankedFeatureVector rfv;
		double[] weights = new double[numFeatures - 1]; // do not deal with the
		// default feature
		for (int li = 0; li < numLabels; li++) {
			out.println();
			out.println("FEATURES FOR CLASS " + labelDict.lookupObject(li)
					+ " ");
			for (int i = 0; i < classifier.getDefaultFeatureIndex(); i++) {
				double weight = classifier.getParameters()[li * numFeatures + i];
				weights[i] = weight;
			}
			rfv = new RankedFeatureVector(dict, weights);
			printTopK(rfv, out, k);

		}
		out.println();
		out.flush();
	}

	/**
	 * 
	 * @param v
	 * @param out
	 * @param num
	 */
	private void printTopK(RankedFeatureVector v, PrintWriter out, int num) {
		int length = v.numLocations();
		if (num > length)
			num = length;
		for (int rank = 0; rank < num; rank++) {
			int idx = v.getIndexAtRank(rank);
			double val = v.getValueAtRank(rank);
			Object obj = v.getAlphabet().lookupObject(idx);
			out.println(obj + "\t" + String.format("%f", val));
		}
	}

	/**
	 * 
	 * @return
	 */
	private Pipe buildFeatureSequencePipe() {
		ArrayList<Pipe> pipeList = new ArrayList<Pipe>();
		pipeList.add(new CharSequence2TokenSequence("[\\S]+"));

		// Normalize all tokens to all lowercase
		pipeList.add(new TokenSequenceLowercase());
		// pipeList.add(new VerbStemmingPipe());
		// Do the same thing for the "target" field:
		// convert a class label string to a Label object,
		// which has an index in a Label alphabet.
		pipeList.add(new Target2Label());

		// Remove stopwords from a standard English stoplist.
		// options: [case sensitive] [mark deletions]
		// pipeList.add(new TokenSequenceRemoveStopwords(false, false));

		// pipeList.add(new TokenTextCharNGrams("CHARNGRAM=", new int[] { 3}));

		// Rather than storing tokens as strings, convert
		// them to integers by looking them up in an alphabet.
		pipeList.add(new TokenSequence2FeatureSequence());
		//
		//		
		// pipeList.add(new FeatureCountPipe());
		//
		// // Now convert the sequence of features to a sparse vector,
		// // mapping feature IDs to counts.
		// pipeList.add(new FeatureSequence2FeatureVector());

		// Print out the features and the label
		// pipeList.add(new PrintInputAndTarget());

		return new SerialPipes(pipeList);
	}

	/**
	 * 
	 * @param stoppWords
	 * @param prunedWords
	 * @return
	 */
	private Pipe buildPipe(File stoppWords, File prunedWords) {
		ArrayList<Pipe> pipeList = new ArrayList<Pipe>();
		pipeList.add(new CharSequence2TokenSequence("[\\S]+"));

		// Normalize all tokens to all lowercase
		pipeList.add(new TokenSequenceLowercase());

		// Do the same thing for the "target" field:
		// convert a class label string to a Label object,
		// which has an index in a Label alphabet.
		pipeList.add(new Target2Label());

		if (prunedWords != null)
			pipeList.add(new TokenSequenceRemoveStopwords(prunedWords, "utf-8",
					false, true, false));
		// Remove stopwords from a standard English stoplist.
		// options: [case sensitive] [mark deletions]

		if (stoppWords != null)
			pipeList.add(new TokenSequenceRemoveStopwords(stoppWords, "utf-8",
					false, true, false));

		// pipeList.add(new TokenTextCharNGrams("CHARNGRAM=", new int[] { 3}));

		// Rather than storing tokens as strings, convert
		// them to integers by looking them up in an alphabet.
		pipeList.add(new TokenSequence2FeatureSequence());
		//
		//		
		//
		// // Now convert the sequence of features to a sparse vector,
		// // mapping feature IDs to counts.
		pipeList.add(new FeatureSequence2FeatureVector());

		// Print out the features and the label
		// pipeList.add(new PrintInputAndTarget());

		return new SerialPipes(pipeList);
	}

	public MaxEnt getClassifier() {
		return classifier;
	}

}

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
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import cc.mallet.fst.CRF;
import cc.mallet.fst.CRFTrainerByLabelLikelihood;
import cc.mallet.fst.MultiSegmentationEvaluator;
import cc.mallet.fst.TransducerTrainer;
import cc.mallet.pipe.Pipe;
import cc.mallet.pipe.SerialPipes;
import cc.mallet.pipe.TokenSequence2FeatureVectorSequence;
import cc.mallet.pipe.iterator.LineGroupIterator;
import cc.mallet.pipe.tsf.FeaturesInWindow;
import cc.mallet.pipe.tsf.TokenText;
import cc.mallet.types.FeatureVectorSequence;
import cc.mallet.types.Instance;
import cc.mallet.types.InstanceList;
import cc.mallet.types.LabelSequence;
import cc.mallet.types.Sequence;
import de.dfki.km.perspecting.obie.model.NounPhraseChunkerPipe;

/*******************************************************************************
 * This class trains a conditional random field (CRF) for the task of noun
 * phrase chunking.
 * 
 * @author sebert
 */
public class NounPhraseChunker {
	/***************************************************************************
	 * Log handler.
	 */
	private static Logger log = Logger.getLogger(NounPhraseChunker.class
			.getName());

	private String crfFile = "";

	private Pipe p = null;

	private InstanceList testData = null;

	private String testFile = "";

	private String trainFile = "";

	private InstanceList trainingData = null;

	private int windowSize = 3; // Window = +- windowSize

	private CRF crf;

	/***************************************************************************
	 * @param trainFile
	 * @param testFile
	 * @param crfFile
	 * @param windowSize
	 */
	public NounPhraseChunker(String trainFile, String testFile, String crfFile,
			int windowSize) {
		super();
		this.trainFile = trainFile;
		this.testFile = testFile;
		this.crfFile = crfFile != null ? crfFile : trainFile + "-"
				+ Integer.toString(windowSize) + ".crf";
		this.windowSize = windowSize;
	}

	public NounPhraseChunker(String crfFile) throws Exception {
		super();
		this.crfFile = crfFile != null ? crfFile : trainFile + "-"
				+ Integer.toString(windowSize) + ".crf";
		ObjectInputStream ois = new ObjectInputStream(new FileInputStream(
				crfFile));
		this.crf = (CRF) ois.readObject();
		ois.close();
		this.p = crf.getInputPipe();
	}
	
	
	public NounPhraseChunker(InputStream input) throws Exception {
		super();
		ObjectInputStream ois = new ObjectInputStream(input);
		this.crf = (CRF) ois.readObject();
		ois.close();
		this.p = crf.getInputPipe();
	}


	/***************************************************************************
	 * Getter of the property <tt>crfFile</tt>
	 * 
	 * @return the crfFile
	 */
	public String getCrfFile() {
		return crfFile;
	}

	/***************************************************************************
	 * Getter of the property <tt>testData</tt>
	 * 
	 * @return the testData
	 */
	public InstanceList getTestData() {
		return testData;
	}

	/***************************************************************************
	 * Getter of the property <tt>testFile</tt>
	 * 
	 * @return the testFile
	 */
	public String getTestFile() {
		return testFile;
	}

	/***************************************************************************
	 * Getter of the property <tt>trainFile</tt>
	 * 
	 * @return the trainFile
	 */
	public String getTrainFile() {
		return trainFile;
	}

	/***************************************************************************
	 * Getter of the property <tt>trainingData</tt>
	 * 
	 * @return the trainingData
	 */
	public InstanceList getTrainingData() {
		return trainingData;
	}

	/***************************************************************************
	 * Getter of the property <tt>windowSize</tt>
	 * 
	 * @return the windowSize
	 */
	public int getWindowSize() {
		return windowSize;
	}

	/***************************************************************************
	 * Getter of the property <tt>crfFile</tt>
	 * 
	 * @param crfFile
	 *            the crfFile to set
	 */
	public void setCrfFile(String crfFile) {
		this.crfFile = crfFile;
	}

	/***************************************************************************
	 * Getter of the property <tt>testFile</tt>
	 * 
	 * @param testFile
	 *            the testFile to set
	 */
	public void setTestFile(String testFile) {
		this.testFile = testFile;
	}

	/***************************************************************************
	 * Getter of the property <tt>trainFile</tt>
	 * 
	 * @param trainFile
	 *            the trainFile to set
	 */
	public void setTrainFile(String trainFile) {
		this.trainFile = trainFile;
	}

	/***************************************************************************
	 * Getter of the property <tt>windowSize</tt>
	 * 
	 * @param windowSize
	 *            the windowSize to set
	 */
	public void setWindowSize(int windowSize) {
		this.windowSize = windowSize;
	}

	/***************************************************************************
	 * Load the stored CRF and perform the test on the training data as well as
	 * on the test data.
	 */
	public void test() throws Exception {
		// Load the crf.
		ObjectInputStream ois = new ObjectInputStream(new FileInputStream(
				crfFile));
		CRF crf = (CRF) ois.readObject();
		ois.close();

		if (trainingData != null) {
			log.info("test on training data");
			testCrf(crf, trainingData);
		}
		if (testData != null) {
			log.info("test on test data");
			testCrf(crf, testData);
		}
	}

	/**
	 * Decides for every token in the text of the given reader if it is inside,
	 * outside, or the beginning of a phrase.
	 * 
	 * @param reader
	 * @return
	 * @throws Exception
	 */
	public List<String> test(Reader reader) throws Exception {
		return testCrf(getData(reader));
	}

	/***************************************************************************
	 * Trains the crf using the initialized pipe and instance lists.
	 */
	public void train() throws Exception {
		log.info("Instance Data (train): " + trainingData.size());
		log.info("Instance Data (test): " + testData.size());

		// Print out all the target names
//		Alphabet targets = p.getTargetAlphabet();
		// String outString = "State labels:";
		//
		// for (int i = 0; i < targets.size(); i++)
		// outString += " " + targets.lookupObject(i);
		//
		// outString += "\n";

		// log.info(outString);
		// log.info("Number of features = " + p.getDataAlphabet().size());

		CRF crf = new CRF(p, null);
		crf.addStatesForLabelsConnectedAsIn(trainingData);

		CRFTrainerByLabelLikelihood trainer = new CRFTrainerByLabelLikelihood(
				crf);
		MultiSegmentationEvaluator evaluator = new MultiSegmentationEvaluator(
				new InstanceList[] { trainingData, testData }, new String[] {
						"Training", "Testing" }, p.getTargetAlphabet()
						.toArray(), p.getTargetAlphabet().toArray()) {
			@Override
			public boolean precondition(TransducerTrainer tt) {
				// evaluate model every 20 training iterations
				return tt.getIteration() % 20 == 0;
			}
		};
		trainer.addEvaluator(evaluator);
		trainer.train(trainingData, Integer.MAX_VALUE);
		evaluator.evaluate(trainer);

		// Store the crf.
		ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(
				crfFile));
		oos.writeObject(crf);
		oos.close();
	}

	/***************************************************************************
	 * Creates the pipe that is necessary to use for the CRF.
	 * 
	 * @return pipe
	 */
	private Pipe buildPipe() {
		ArrayList<Pipe> pipeList = new ArrayList<Pipe>();
		pipeList.add(new NounPhraseChunkerPipe());
		// Use token text as additional feature.
		pipeList.add(new TokenText("W="));
		// Use the POS tags of the neighboring tokens as feature for this token.
		pipeList.add(new FeaturesInWindow("WINDOW=", -windowSize, windowSize,
				Pattern.compile("T=.*"), true));
		// Use the token text of the neighboring tokens as feature for this
		// token.
		pipeList.add(new FeaturesInWindow("WINDOW=", -windowSize, windowSize,
				Pattern.compile("W=.*"), true));
		pipeList.add(new TokenSequence2FeatureVectorSequence(true, true));
		// pipeList.add(new PrintInputAndTarget());
		return new SerialPipes(pipeList);
	}

	/***************************************************************************
	 * Reads the instance data out of the given file.
	 * 
	 * @param file
	 *            file to read the instance information from
	 * @return Instances that were loaded out of the given file.
	 * @throws Exception
	 */
	private InstanceList getData(String file) throws Exception {
		InstanceList data = new InstanceList(p);

		data.addThruPipe(new LineGroupIterator(new FileReader(new File(file)),
				Pattern.compile("^\\s*$"), true));
		return data;
	}

	private InstanceList getData(Reader input) throws Exception {
		InstanceList data = new InstanceList(p);

		data.addThruPipe(new LineGroupIterator(input,
				Pattern.compile("^\\s*$"), true));
		return data;
	}

	/***************************************************************************
	 * Initializes the pipe and instance lists.
	 * 
	 * @throws Exception
	 */
	public void init() throws Exception {
		p = buildPipe();

		// Do we have different files for training and testing? If not, we need
		// to split the data of the file into training and testing.
		if (!trainFile.equals(testFile)) {
			trainingData = getData(trainFile);
			testData = getData(testFile);
		} else {
			trainingData = getData(trainFile);
			InstanceList[] instanceLists = trainingData.split(new Random(),
					new double[] { 0.8, 0.2, 0.0 });
			trainingData = instanceLists[0];
			testData = instanceLists[1];
		}
	}

	/***************************************************************************
	 * Performes the test on the given data using the given CRF.
	 * 
	 * @param crf
	 * @param data
	 */
	private void testCrf(CRF crf, InstanceList data) {
		int correct = 0;
		int count = 0;

		for (Instance curInst : data) {
			Sequence<String> output = crf
					.transduce((FeatureVectorSequence) curInst.getData());
			LabelSequence target = ((LabelSequence) curInst.getTarget());
			count += output.size();

			for (int i = 0; i < output.size(); i++) {

				if ((target.getLabelAtPosition(i).getEntry().equals(output
						.get(i))))
					++correct;

				// log.info(target.getLabelAtPosition(i).getEntry() + " / " +
				// output.get(i) + "\t");
			}
		}

		log.info("accuracy (correct / total): " + correct + " / " + count
				+ " = " + ((double) correct / count));
	}

	private List<String> testCrf(InstanceList data) {
		ArrayList<String> np = new ArrayList<String>();

		for (Instance curInst : data) {
			Sequence<String> output = crf
					.transduce((FeatureVectorSequence) curInst.getData());

			for (int i = 0; i < output.size(); i++) {
				np.add(output.get(i));
			}
		}

		return np;
	}
}

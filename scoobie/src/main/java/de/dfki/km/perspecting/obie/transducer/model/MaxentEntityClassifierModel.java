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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Logger;

import cc.mallet.classify.MaxEnt;
import de.dfki.km.perspecting.obie.connection.KnowledgeBase;
import de.dfki.km.perspecting.obie.corpus.LabeledTextCorpus;
import de.dfki.km.perspecting.obie.vocabulary.Language;

public class MaxentEntityClassifierModel {

	private final Logger log = Logger
			.getLogger(MaxentEntityClassifierModel.class.getName());

	private final String modelPath;
	private final Language language;

	private EntityClassifier classifier = null;

	private int[] ngramsize;

	private boolean useContext;

	private boolean useContent;

	private boolean useRegex;

	private double typeProportion;

	private int windowSize;

	private String[] postags;

	public MaxentEntityClassifierModel(String modelPath, Language language,
			final int[] ngramsize, final boolean useContext,
			final boolean useContent, final boolean useRegex,
			final double typeProportion, final int windowSize,
			final String... postags) {
		this.modelPath = modelPath;
		this.language = language;
		this.ngramsize = ngramsize;
		this.useContext = useContext;
		this.useContent = useContent;
		this.useRegex = useRegex;
		this.typeProportion = typeProportion;
		this.windowSize = windowSize;
		this.postags = postags;
	}
	
	public void load(KnowledgeBase kb, LabeledTextCorpus groundTruth)
			throws Exception {
		File model = new File(modelPath);
		if (!model.exists()) {
			log.info("MaxEnt(" + language.getValue()
							+ ") classifier does not exist at: "
							+ model.getAbsolutePath());
			train(kb, groundTruth);
		}
		if (model.exists()) {
			log.info("Loading " + language.getValue() + " MaxEnt classifier");

			MaxEnt nativeCLassifier = loadObject(model);
//			nativeCLassifier.getLabelAlphabet().dump(System.out);
			this.classifier = new EntityClassifier(nativeCLassifier);
		} else {
			log.info("Skipped loading " + language.getValue()
							+ " MaxEnt classifier");
		}
	}

	/**
	 * @param model
	 * @return
	 * @throws IOException
	 * @throws FileNotFoundException
	 * @throws ClassNotFoundException
	 */
	private <T> T loadObject(File model) throws IOException,
			FileNotFoundException, ClassNotFoundException {
		ObjectInputStream in = new ObjectInputStream(new FileInputStream(model));
		T nativeCLassifier = (T) in.readObject();
		in.close();
		return nativeCLassifier;
	}

	public void train(KnowledgeBase kb, LabeledTextCorpus corpus)
			throws Exception {
		log.info("Start training " + language.getValue() + " MaxEnt classifier");
		EntityClassifier classifier = new EntityClassifier(null);
		trainClassifier(classifier, kb, corpus);
	}

	/**
	 * @param classifier
	 * @param buffer
	 * @throws Exception
	 * @throws IOException
	 * @throws FileNotFoundException
	 */
	private void trainClassifier(EntityClassifier classifier, KnowledgeBase kb,
			LabeledTextCorpus corpus) throws Exception {

		StringBuilder b = new StringBuilder();

		Reader reader = corpus.toFeatureFormat(new File(corpus.getCorpus()
				.getParentFile().getAbsolutePath()
				+ "/" + corpus.getCorpus().getName() + ".labeled"), ngramsize,
				useContext, useContent, useRegex, typeProportion, windowSize,
				postags);

		Reader r = new BufferedReader(reader);
		Map<String, Double[]> results = classifier.evaluate(r, null, null,
				null, true, 0.9);

		for (Entry<String, Double[]> e : results.entrySet()) {
			log.info(String.format("%s \t %f \t %f \t %f \n", kb.getURI(Integer
					.parseInt(e.getKey())), e.getValue()[0], e.getValue()[1], e
					.getValue()[2]));
		}
		r.close();

		r = new BufferedReader(new StringReader(b.toString()));
		classifier.train(r, null, null);
		serializeObject(classifier.getClassifier(), modelPath);
		r.close();
	}

	/**
	 * @throws IOException
	 * @throws FileNotFoundException
	 */
	private void serializeObject(Object object, String path)
			throws IOException, FileNotFoundException {
		ObjectOutputStream w = new ObjectOutputStream(
				new FileOutputStream(path));
		w.writeObject(object);
		w.close();
	}
	
	public EntityClassifier getClassifier() {
		return classifier;
	}

}

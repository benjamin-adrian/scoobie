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

package de.dfki.km.perspecting.obie.transducer;

import java.io.BufferedReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.logging.Logger;

import cc.mallet.classify.Classification;
import de.dfki.km.perspecting.obie.connection.KnowledgeBase;
import de.dfki.km.perspecting.obie.corpus.LabeledTextCorpus;
import de.dfki.km.perspecting.obie.model.Document;
import de.dfki.km.perspecting.obie.model.SemanticEntity;
import de.dfki.km.perspecting.obie.model.Token;
import de.dfki.km.perspecting.obie.model.TokenSequence;
import de.dfki.km.perspecting.obie.transducer.model.EntityClassifier;
import de.dfki.km.perspecting.obie.workflow.Transducer;

public class EntityClassification extends Transducer {

	private final Logger log = Logger.getLogger(EntityClassification.class
			.getName());
	private double threshold;

	private EntityClassifier model;
	
	public EntityClassification(double threshold, EntityClassifier model) {
		this.threshold = threshold;
		this.model = model;
	}

	@Override
	public void transduce(Document record, KnowledgeBase kb)
			throws Exception {

		if (model != null) {
			List<TokenSequence<String>> phrases = record.getNounPhrases();
			int exampleCount = 0;
			for (TokenSequence<String> phrase : phrases) {
				HashSet<Integer> textIndexes = new HashSet<Integer>();
				for (Token t : phrase.getTokens()) {
					textIndexes.add(t.getStart());
				}

				Classification cl = classify(exampleCount++, phrase, model);

				int typeIndex = -1;
				double val = cl.getLabelVector().getValueAtRank(0)
						- cl.getLabelVector().getValueAtRank(1);
				if (val > threshold) {
					typeIndex = Integer.parseInt(cl.getLabeling()
							.getBestLabel().toString());
					for (int t = 0; t < phrase.getTokens().size(); t++) {
						Token token = phrase.getTokens().get(t);

						SemanticEntity e = new SemanticEntity();
						e.setSubjectIndex(-1);
						e.setSubjectURI("_:bnode");
						if (t == 0) {
							e.setPosition("B");
						} else {
							e.setPosition("I");
						}

						token.addType(e, typeIndex, val);

					}

				}
			}
		}

	}

	/**
	 * @param exampleCount
	 * @param entity
	 * @return
	 * @throws Exception
	 */
	private Classification classify(int exampleCount, TokenSequence<?> entity,
			EntityClassifier classifier) throws Exception {
		Token firstToken = entity.getTokens().get(0);
		TokenSequence<Integer> sentence = firstToken.getSentence();
		int tokenCount = entity.getTokens().size();
		int i = 0;
		List<Integer> labelIndexes = new ArrayList<Integer>();
		List<String[]> featureSentence = new ArrayList<String[]>();
		for (Token t : sentence.getTokens()) {
			if (t.equals(firstToken)) {
				for (int j = i; j < i + tokenCount; j++) {
					labelIndexes.add(j);
				}
			}

			String[] feature = new String[3];
			feature[0] = t.toString();
			feature[1] = t.getPartOfSpeechTag();
			feature[2] = t.getNounPhraseTag();
			featureSentence.add(feature);
			i++;
		}

		List<String> list = LabeledTextCorpus.extractFeatures(labelIndexes,
				featureSentence, new int[] { 1, 2, 3 }, true, true, true, 1.0,
				4, new String[] { "VB", "ADJ", "NNP", "NN", "NNS" });

		StringBuilder corpusWriter = new StringBuilder();
		corpusWriter.append(Integer.toString(exampleCount++));
		corpusWriter.append(" ");
		// this is a hack to pass mallet's vocabulary match
		corpusWriter.append(classifier.getClassifier().getLabelAlphabet()
				.lookupObject(0).toString());
		for (String ft : list) {
			corpusWriter.append(" ");
			corpusWriter.append(ft);
		}
		corpusWriter.append("\n");

		List<Classification> classification = classifier.test(corpusWriter
				.toString());
		return classification.get(0);
	}

	/**
	 * Input data in line based form:
	 * 
	 * literal \t type \n
	 * 
	 * @return
	 */
	public String compare(Document document, KnowledgeBase kb, Reader gt)
			throws Exception {

		HashSet<String> relevantLines = new HashSet<String>();

		BufferedReader br = new BufferedReader(gt);
		for (String line = br.readLine(); line != null; line = br.readLine()) {
			relevantLines.add(line);
		}

		HashSet<String> foundLines = new HashSet<String>();

		for (TokenSequence<SemanticEntity> ts : document.getEntityTypes()) {
			SemanticEntity se = ts.getValue();
			String uri = kb.getURI(se.getTypeIndex().get(0).getKey());
			String value = ts.toString();

			foundLines.add(value + "\t" + uri);
		}

		HashSet<String> foundRelevantLines = new HashSet<String>(relevantLines);
		foundRelevantLines.retainAll(foundLines);

		double recall = ((double) foundRelevantLines.size())
				/ relevantLines.size();
		double precision = ((double) foundRelevantLines.size())
				/ foundLines.size();

		return String.format("%i\t%i\t%d\t%d\n", relevantLines.size(),
				foundLines.size(), recall, precision);
	}

}

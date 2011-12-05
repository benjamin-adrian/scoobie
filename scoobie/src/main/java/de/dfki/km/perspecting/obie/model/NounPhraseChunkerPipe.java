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

import cc.mallet.pipe.Pipe;
import cc.mallet.types.Instance;
import cc.mallet.types.LabelAlphabet;
import cc.mallet.types.LabelSequence;
import cc.mallet.types.Token;
import cc.mallet.types.TokenSequence;

public class NounPhraseChunkerPipe extends Pipe {
	boolean saveSource = false;
	boolean doConjunctions = false;

	public NounPhraseChunkerPipe() {
		super(null, new LabelAlphabet());
	}

	/*
	 * Lines look like this: -DOCSTART- -X- -X- O
	 * 
	 * EU NNP I-NP I-ORG rejects VBZ I-VP O German JJ I-NP I-MISC call NN I-NP O
	 * to TO I-VP O boycott VB I-VP O British JJ I-NP I-MISC lamb NN I-NP O . .
	 * O O
	 * 
	 * Peter NNP I-NP I-PER Blackburn NNP I-NP I-PER
	 * 
	 * BRUSSELS NNP I-NP I-LOC 1996-08-22 CD I-NP O
	 * 
	 * The DT I-NP O European NNP I-NP I-ORG Commission NNP I-NP I-ORG said VBD
	 * I-VP O on IN I-PP O ...
	 */

	public Instance pipe(Instance carrier) {
		String sentenceLines = (String) carrier.getData();
		String[] tokens = sentenceLines.split("\n");
		TokenSequence data = new TokenSequence(tokens.length);
		LabelSequence target = new LabelSequence(
				(LabelAlphabet) getTargetAlphabet(), tokens.length);
		StringBuffer source = saveSource ? new StringBuffer() : null;

		String word, tag, phrase;
		for (int i = 0; i < tokens.length; i++) {
			if (tokens[i].length() != 0) {
				String[] features = tokens[i].split(" ");
				if (features.length == 3) {
					word = features[0]; // .toLowerCase();
					tag = features[1];
					phrase = features[2];
				} else {
					word = features[0]; // .toLowerCase();
					tag = features[1];
					phrase = "-<S>-";
				}

			} else {
				word = "-<S>-";
				tag = "-<S>-";
				phrase = "-<S>-";
			}

			Token token = new Token(word);

			token.setFeatureValue("T=" + tag, 1);
			// Append
			data.add(token);
			// target.add (bigramLabel);
			target.add(phrase);
			// System.out.print (label + ' ');
			if (saveSource) {
				source.append(word);
				source.append(" ");
				// source.append (bigramLabel); source.append ("\n");
				source.append(phrase);
				source.append("\n");
			}

		}
		// System.out.println ("");
		carrier.setData(data);
		carrier.setTarget(target);
		if (saveSource)
			carrier.setSource(source);
		return carrier;
	}
}

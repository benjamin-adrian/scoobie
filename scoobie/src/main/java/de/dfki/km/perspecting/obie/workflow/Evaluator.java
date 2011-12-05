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

package de.dfki.km.perspecting.obie.workflow;

import java.io.Writer;

import de.dfki.km.perspecting.obie.corpus.LabeledTextCorpus;
import de.dfki.km.perspecting.obie.model.Document;

public class Evaluator {

	private Pipeline pipeline;

	public Evaluator(Pipeline pipeline) {
		this.pipeline = pipeline;
	}

	public void evaluate(int step, Document document, LabeledTextCorpus gt,
			Writer report) throws Exception {
		Transducer tranducer = pipeline.getTranducer(step);
		report.append(tranducer.compare(document, pipeline.getKnowledgeBase(),
				gt.getGroundTruth(document.getUri())));
		report.flush();
	}

}

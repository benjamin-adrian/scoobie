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

import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.analysis.lang.LanguageIdentifier;

import de.dfki.km.perspecting.obie.connection.KnowledgeBase;
import de.dfki.km.perspecting.obie.model.Document;
import de.dfki.km.perspecting.obie.vocabulary.Language;
import de.dfki.km.perspecting.obie.workflow.Transducer;

/**
 * A language classifier based on a Nutch plugin.
 * 
 * 
 * @author adrian
 * 
 */
public class LanguageIdentification extends Transducer {

	private final LanguageIdentifier li;
	private final Logger log = Logger.getLogger(LanguageIdentification.class
			.getName());
	private final Language defaultLanguage;

	/**
	 * Creates a new language identifier and set a default language.
	 * 
	 * @param defaultLanguage
	 */
	public LanguageIdentification(Language defaultLanguage) {
		this.li = new LanguageIdentifier(new Configuration());
		this.defaultLanguage = defaultLanguage;
	}

	public String identifyLanguageFromText(String text) {
		return li.identify(text);
	}

	/**
	 * Classifies a text as English or German. Default language is English
	 */
	public void transduce(Document docData, KnowledgeBase kb)
			throws Exception {

		if (docData.getLanguage() == Language.UNKNOWN) {
			String language = li.identify(docData.getPlainTextContent());

			log.info("identified language as " + language);

			if (language.equals(Language.DE.getValue())) {
				docData.setLanguage(Language.DE);
			} else if (language.equals(Language.EN.getValue())) {
				docData.setLanguage(Language.EN);
			} else {
				log.info("set language to default: " + defaultLanguage);
				docData.setLanguage(defaultLanguage);
			}
		}
	}

}

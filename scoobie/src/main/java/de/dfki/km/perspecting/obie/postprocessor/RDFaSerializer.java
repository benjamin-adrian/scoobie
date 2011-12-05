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

package de.dfki.km.perspecting.obie.postprocessor;

import java.io.Reader;
import java.io.StringReader;
import java.util.List;

import de.dfki.km.perspecting.obie.connection.KnowledgeBase;
import de.dfki.km.perspecting.obie.model.Document;
import de.dfki.km.perspecting.obie.model.SemanticEntity;
import de.dfki.km.perspecting.obie.model.Token;

/**
 * 
 * {@link RDFaSerializer} formats extracted results in form of RDFa.
 * In terms of a document with text/plain as mimetype, the result
 * is converted into text/html+rdfa. If it is an HTML document,
 * RDFa content is embedded into the existing markup. Please note that the mediatype
 * of the original HTML code is not changed.
 * 
 * @author adrian
 *
 */
public class RDFaSerializer implements Serializer {

	private static final int snippetLength = 20;

	@Override
	public Reader serialize(Document document, KnowledgeBase kb)
			throws Exception {

		switch (document.getMimeType()) {

		case TEXT:
			return serializeTextPlain(document, kb, document.getContent());
		case HTML:
			return serializeTextHtml(document, kb, document.getContent());

		default:
			return serializeTextPlain(document, kb, document.getContent());
		}

	}

	/**
	 * Returns a {@link Reader} containing the document content in RDFa format.
	 * @param text the original text content.
	 */
	private Reader serializeTextPlain(Document document, KnowledgeBase kb,
			String text) {

		StringBuilder content = new StringBuilder();

		String snippet = document.getPlainTextContent().substring(0,
				Math.min(snippetLength, document.getPlainTextContent().length()));

		content
				.append("<?xml version='1.0' encoding='UTF-8'?>\n"
						+ "<!DOCTYPE html PUBLIC '-//W3C//DTD XHTML+RDFa 1.0//EN' 'http://www.w3.org/MarkUp/DTD/xhtml-rdfa-1.dtd'>\n"
						+ "<html xmlns=\"http://www.w3.org/1999/xhtml\">\n"
						+ "<head>\n"
						+ "  <title>"
						+ snippet
						+ "...</title>\n"
						+ "  <meta http-equiv=\"Content-Type\" content=\"text/html;charset=utf-8\"/>\n"
						+ "</head>\n");

		content.append("<body>\n");
		annotate(document, kb, text, content);
		content.append("\n</body>\n");
		content.append("</html>\n");
		return new StringReader(content.toString());

	}

	/**
	 * Returns a {@link Reader} containing the document content in RDFa format.
	 * @param text the original text content.
	 */
	private Reader serializeTextHtml(Document document, KnowledgeBase kb,
			String text) {
		StringBuilder content = new StringBuilder();
		annotate(document, kb, text, content);
		return new StringReader(content.toString());

	}

	/**
	 * Annotates text content with SPAN elements and
	 * RDFa content.
	 * @param text the original text content.
	 */
	private void annotate(Document document, KnowledgeBase kb, String text, StringBuilder content) {
		int positionInText = 0;

		for (Token t : document) {

			int start = t.getStart();
			int end = t.getEnd();

			if (positionInText < start) {
				content.append(text.subSequence(positionInText, start));
			}

			List<SemanticEntity> e = t.getAnnotations();

			if (e.isEmpty()) {
				content.append(t);
			} else {
				SemanticEntity entity = e.get(0);

				content.append("<span ");
				String uri = entity.getSubjectURI();
				if (uri != null) {
					content.append("about=\"" + uri + "\"");
				}
				String property = "";
				try {
					property = kb.getURI(entity.getPropertyIndex());
				} catch (Exception e1) {assert false : "must not occur";}
				content.append(" property=\"" + property + "\"");
				content.append(">");
				content.append(t.toString());
				content.append("</span>");
			}

			positionInText = end;
		}
		
		if (positionInText < text.length()) {
			content.append(text.substring(positionInText));
		}

		
	}

}

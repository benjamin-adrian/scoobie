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

package de.dfki.km.perspecting.obie.connection;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.RDFParserFactory;
import org.openrdf.rio.RDFParserRegistry;

import de.dfki.km.perspecting.obie.vocabulary.MediaType;

public class RDFTripleParser {

	/**
	 * 
	 */
	private static final String BASEURI = "http://www.dfki.de";

	private static final Logger log = Logger.getLogger(RDFTripleParser.class
			.getName());

	private static final String ALL = "all";

	private static final ExecutorService pool = Executors.newCachedThreadPool();

	private static final int URISIZE = 120;

	final Object SEMAPHOR = new Object();

	private static InputStream getStream(InputStream stream, MediaType mediatype)
			throws Exception {

		switch (mediatype) {
		case BZIP:
			return new BZip2CompressorInputStream(stream);
		case GZIP:
			return new GzipCompressorInputStream(stream);
		case ZIP:
			return new ZipArchiveInputStream(stream);

		default:
			return stream;

		}
	}

	static class TripleStats {
		// public File literalLanguageList;

		public File datatypeProps;

		public File objectProps;

		public volatile int datatypePropsSize = 0;

		public volatile int objectPropsSize = 0;

	}

	private static char[] encloseCharacterString(String uri) {
		char[] c = new char[uri.length() + 2];
		c[0] = '"';
		System.arraycopy(uri.toCharArray(), 0, c, 1, uri.length());
		c[c.length - 1] = '"';
		return c;
	}

	/**
	 * 
	 * @param input
	 * @param mimetype
	 * @param sessionPath
	 * @param absoluteBaseURI
	 * @return
	 * @throws Exception
	 */
	public TripleStats parseTriples(final InputStream[] input,
			final MediaType rdf_mimetype, final File sessionPath,
			final String absoluteBaseURI, final MediaType file_mimetype)
			throws Exception {

		final TripleStats stats = new TripleStats();
//		int count = 0;

		new File(sessionPath.getAbsolutePath() + "/dump/").mkdirs();

		stats.datatypeProps = new File(sessionPath.getAbsolutePath()
				+ "/dump/datatypeProperties.lst");
		stats.objectProps = new File(sessionPath.getAbsolutePath() + "/dump/objectProperties.lst");

		stats.datatypeProps.deleteOnExit();
		stats.objectProps.deleteOnExit();
		
		stats.datatypeProps.setReadable(true, false);
		stats.objectProps.setReadable(true, false);

		if (stats.datatypeProps.exists() && stats.objectProps.exists()) {
			return stats;
		}

		// stats.literalLanguageList = new File(sessionPath +
		// "/dump/literals_language.lst");

		// final BufferedWriter literalLanguageWriter = new BufferedWriter(new
		// FileWriter(
		// stats.literalLanguageList, true));

		final BufferedWriter datatypePropertiesWriter = new BufferedWriter(
				new FileWriter(stats.datatypeProps, false));

		final BufferedWriter objectPropertiesWriter = new BufferedWriter(
				new FileWriter(stats.objectProps, false));

		final ArrayList<Callable<Boolean>> threads = new ArrayList<Callable<Boolean>>();

		
		int sourceCount = 0;
		
		for (final InputStream stream : input) {

			final String source = (++sourceCount)+"";
			log.info("Parsing: " + source + " from ( "+ input.length + " )");

			final RDFParser parser = getParser(rdf_mimetype);
			parser.setRDFHandler(new RDFHandler() {

				long tripleCount = 0;

				@Override
				public void startRDF() throws RDFHandlerException {
					log.info("Start parsing RDF triples");
				}

				@Override
				public void handleStatement(Statement stmt)
						throws RDFHandlerException {
					try {

						tripleCount++;

						if (tripleCount % 10000 == 0) {
							log.info(source + ": Parsed " + tripleCount
									+ " RDF triples");
						}
						// get triple components
						String p = stmt.getPredicate().toString();
						String s = stmt.getSubject().toString();
						String o = stmt.getObject().toString();

						// test URIs
						if (s.length() > URISIZE) {
							log.warning("Skipping too long subject " + s);
							return;
						}

						if (p.length() > URISIZE) {
							log.warning("Skipping too long predicate " + p);
							return;
						}

						if (stmt.getSubject() instanceof URI)
							s = fixJavaURI(s);

						p = fixJavaURI(p);

						// check object properties URIs
						if (stmt.getObject() instanceof URI) {
							if (o.length() > URISIZE) {
								return;
							} else {
								o = fixJavaURI(o);
								appendObjectTriple(s, p, o);
							}
						} else if (stmt.getObject() instanceof Literal) {
							o = stmt.getObject().stringValue().replaceAll(
									"[\n\t\\\\\"]", "").trim();

							if (o.length() < 2 || o.length() > 100) {
								return;
							}

							appendLiteralTriple(s, p, o, ((Literal) stmt
									.getObject()).getLanguage());
						} else {
							log.warning("Skipping bad triple " + stmt);
						}

					} catch (Exception e) {
						log.log(Level.SEVERE, "Error in parsing: " + source, e);
					}

				}

				/**
				 * Encodes characters invalid (e.g. "|") in the uri and returns
				 * the encoded string.
				 * 
				 * @param uri
				 *            uri to enctode
				 * @return encoded uri
				 */
				private String fixJavaURI(String uri) {

					try {
						new java.net.URI(uri);
					} catch (URISyntaxException e) {
						String badChar = Character.toString(uri.charAt(e
								.getIndex()));
						try {
							log.fine("Fixing bad uri: " + uri);
							return fixJavaURI(uri.replace(badChar, URLEncoder
									.encode(badChar, "utf-8")));
						} catch (UnsupportedEncodingException e1) {
							throw new RuntimeException(e1);
						}
					}

					return uri;
				}

				private void appendLiteralTriple(String subject,
						String predicate, String literal, String language)
						throws IOException {

					if (language == null) {
						language = ALL;
					}

					synchronized (SEMAPHOR) {
						stats.datatypePropsSize++;
						datatypePropertiesWriter.write(RDFTripleParser
								.encloseCharacterString(subject));
						datatypePropertiesWriter.append(',');
						datatypePropertiesWriter.write(RDFTripleParser
								.encloseCharacterString(predicate));
						datatypePropertiesWriter.append(',');
						datatypePropertiesWriter.write(RDFTripleParser
								.encloseCharacterString(literal));

						// TODO: Code for hashing literals
						datatypePropertiesWriter.append(',');
						int max = Math.min(literal.length(), 4);
						datatypePropertiesWriter.write(Integer.toString(literal.substring(0, max).hashCode()));

						datatypePropertiesWriter.newLine();
					}

				}

				private void appendObjectTriple(String subject,
						String predicate, String object) throws IOException {

					synchronized (SEMAPHOR) {

						stats.objectPropsSize++;

						objectPropertiesWriter.write(RDFTripleParser
								.encloseCharacterString(subject));
						objectPropertiesWriter.append(',');
						objectPropertiesWriter.write(RDFTripleParser
								.encloseCharacterString(predicate));
						objectPropertiesWriter.append(',');
						objectPropertiesWriter.write(RDFTripleParser
								.encloseCharacterString(object));
						objectPropertiesWriter.newLine();
					}

				}

				@Override
				public void handleNamespace(String arg0, String arg1)
						throws RDFHandlerException {
				}

				@Override
				public void handleComment(String arg0)
						throws RDFHandlerException {
				}

				@Override
				public void endRDF() throws RDFHandlerException {
					log.info("Finished parsing RDF triples " + tripleCount + " RDF triples");

				}
			});

			threads.add(new Callable<Boolean>() {
				/*
				 * (non-Javadoc)
				 * 
				 * @see java.util.concurrent.Callable#call()
				 */
				@Override
				public Boolean call() throws Exception {

					InputStream unpackedStream = getStream(stream, file_mimetype);
					try {
						if (absoluteBaseURI != null)
							parser.parse(unpackedStream, absoluteBaseURI);
						else
							parser.parse(unpackedStream, BASEURI);
					} catch (Exception e) {
						new Exception("Error during parsing " + source
								+ " with mimetype " + file_mimetype, e)
								.printStackTrace();
						unpackedStream.close();
						return false;
					}
					unpackedStream.close();

					return true;
				}
			});

		}

		for (Future<Boolean> future : pool.invokeAll(threads)) {
			if (!future.get()) {
				throw new Exception("error occured during parsing");
			}
		}

		// literalLanguageWriter.close();
		objectPropertiesWriter.close();
		datatypePropertiesWriter.close();

		return stats;
	}

	private RDFParser getParser(MediaType mimetype) {
		RDFParserRegistry parserRegistry = RDFParserRegistry.getInstance();
		RDFFormat format = parserRegistry.getFileFormatForMIMEType(mimetype.toString());
		RDFParserFactory parserFactory = parserRegistry.get(format);
		RDFParser parser = parserFactory.getParser();
		parser.setValueFactory(new MyValueFactoryImpl());
		parser.setVerifyData(false);
		parser.setStopAtFirstError(false);
		return parser;
	}

	static class MyValueFactoryImpl extends ValueFactoryImpl {

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * org.openrdf.model.impl.ValueFactoryImpl#createURI(java.lang.String)
		 */
		@Override
		public URI createURI(String uri) {
			try {
				return super.createURI(uri);
			} catch (Exception e) {

				if (uri.contains("|")) {
					try {
						String uri1 = uri.replace("|", URLEncoder.encode("|",
								"utf-8"));
						log.fine("Fixed URI: " + uri + " to " + uri1);
						return createURI(uri1);
					} catch (UnsupportedEncodingException e1) {
						throw new RuntimeException(e1);
					}
				} else {
					log.fine("Fixed URI: "
									+ uri + " to http://" + uri);
					return super.createURI("http://" + uri);
				}
			}
		}

	}

}

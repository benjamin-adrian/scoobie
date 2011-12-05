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

package de.dfki.km.perspecting.obie.preprocessor;

import gnu.trove.TDoubleFunction;
import gnu.trove.TIntDoubleHashMap;
import gnu.trove.TIntIntHashMap;
import gnu.trove.TIntIntProcedure;
import gnu.trove.TIntObjectHashMap;
import gnu.trove.TIntObjectProcedure;

import java.io.Reader;
import java.net.URI;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import de.dfki.km.perspecting.obie.connection.KnowledgeBase;
import de.dfki.km.perspecting.obie.corpus.TextCorpus;
import de.dfki.km.perspecting.obie.model.Document;
import de.dfki.km.perspecting.obie.model.DocumentProcedure;
import de.dfki.km.perspecting.obie.model.SemanticEntity;
import de.dfki.km.perspecting.obie.model.TokenSequence;
import de.dfki.km.perspecting.obie.transducer.RDFLiteralSpotting;
import de.dfki.km.perspecting.obie.workflow.Pipeline;

public class ProperNameMining {

	private final static String VIEW_AMBIGUITY_SYMBOLS = "CREATE OR REPLACE VIEW ambiguity_symbols AS  SELECT symbols.predicate AS attribute, avg(histogram_literals.count) AS avg_references FROM symbols, histogram_literals  WHERE histogram_literals.literal = symbols.object  GROUP BY symbols.predicate";
	private final static String VIEW_HISTOGRAM_LITERALS = "CREATE OR REPLACE VIEW histogram_literals AS SELECT symbols.object AS literal, count(DISTINCT symbols.subject) AS count   FROM symbols  GROUP BY symbols.object";
	private final static String VIEW_HISTOGRAM_TYPES = "CREATE OR REPLACE VIEW histogram_types AS SELECT r.object AS type, count(DISTINCT r.subject) AS count FROM relations r WHERE (r.predicate IN ( SELECT index_resources.index FROM index_resources WHERE index_resources.uri = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type')) GROUP BY r.object";

	private final static String SELECT_COVERAGE_AMBIGUITY = "SELECT C.attribute, C.coverage, A.avg_references FROM ( "
			+ "SELECT S.predicate AS attribute, (count(DISTINCT S.subject) / avg(HT.count)) AS coverage "
			+ "FROM relations R, symbols S, histogram_types HT "
			+ "WHERE ( HT.type = R.object AND S.subject = R.subject AND R.object = ?) "
			+ "GROUP BY S.predicate ) "
			+ "C, AMBIGUITY_SYMBOLS A "
			+ "WHERE ( A.attribute = C.attribute )";

	private final static String DROP_PROPER_NOUN_RATING = "DROP TABLE IF EXISTS proper_noun_rating CASCADE;";
	private final static String CREATE_PROPER_NOUN_RATING = "CREATE TABLE proper_noun_rating (cluster int, property int, rating real, coverage real, ambiguity real, idf real)";
	
	
	private final TextCorpus corpus;
	private final KnowledgeBase kb;
	private final Pipeline pipe;

	public ProperNameMining(TextCorpus corpus, Pipeline pipe) {
		this.kb = pipe.getKnowledgeBase();
		this.corpus = corpus;
		this.pipe = pipe;
	}
	
	
	private void resetDatabase() throws SQLException {

		Connection conn = kb.getConnection();
		conn.setAutoCommit(false);

		conn.createStatement().executeUpdate(DROP_PROPER_NOUN_RATING);
		conn.createStatement().executeUpdate(CREATE_PROPER_NOUN_RATING);
		conn.createStatement().executeUpdate(VIEW_HISTOGRAM_LITERALS);
		conn.createStatement().executeUpdate(VIEW_HISTOGRAM_TYPES);
		conn.createStatement().executeUpdate(VIEW_AMBIGUITY_SYMBOLS);
		
		conn.commit();
		conn.setAutoCommit(true);
	}
	
	public void train() throws Exception {
		 resetDatabase() ;
		final TIntDoubleHashMap propertyIDF =  getDocumentFrequency();
		final TIntObjectHashMap<TIntObjectHashMap<double[]>> typePropertyCoverageAmbiguity =  getCoverageAmbiguity();
	
		final String sql = "INSERT INTO proper_noun_rating VALUES (?,?,?,?,?,?)";
		
		final PreparedStatement pstmt = kb.getConnection().prepareStatement(sql);
		
		typePropertyCoverageAmbiguity.forEachEntry(new TIntObjectProcedure<TIntObjectHashMap<double[]>>() {
			@Override
			public boolean execute(final int type, TIntObjectHashMap<double[]> propertyCoverageAmbiguity) {
				
				propertyCoverageAmbiguity.forEachEntry(new TIntObjectProcedure<double[]>() {
					
					@Override
					public boolean execute(int property, double[] coverageAmbiguity) {
						
						double idf = propertyIDF.get(property);

						try {
							pstmt.setInt(1, type);
							pstmt.setInt(2, property);
							pstmt.setDouble(3, coverageAmbiguity[0] / coverageAmbiguity[1] * idf);
							pstmt.setDouble(4, coverageAmbiguity[0]); 
							pstmt.setDouble(5, coverageAmbiguity[1]); 
							pstmt.setDouble(6, idf); 
							pstmt.executeUpdate();
						} catch (SQLException e) {
							e.printStackTrace();
						}
						
						
						return true;
					}
				});
				
				return true;
			}
		});
		
		pstmt.close();
		
	}

	@SuppressWarnings("unchecked")
	private  TIntDoubleHashMap getDocumentFrequency() throws Exception {

		// calculate property frequency per document
		final List<TIntIntHashMap> results = (List<TIntIntHashMap>) corpus
				.forEach(new DocumentProcedure<TIntIntHashMap>() {

					@Override
					public TIntIntHashMap process(final Reader file,
							final URI uri) throws Exception {

						final Document document = pipe.createDocument(file, uri, corpus.getMediatype(), "SELECT * WHERE {?s ?p ?o}", corpus.getLanguage());

						final TIntIntHashMap stats = new TIntIntHashMap();
						for (int step = 0; pipe.hasNext(step) && step < 8; step = pipe
								.execute(step, document)) {
							if (step > 0
									&& pipe.getTranducer(step - 1).getClass()
											.equals(RDFLiteralSpotting.class)) {
								for (final TokenSequence<SemanticEntity> se : document
										.getRetrievedPropertyValues()) {
									stats.adjustOrPutValue(se.getValue()
											.getPropertyIndex(), 1, 1);
								}
								break;
							}
						}
						return stats;
					}
				});

		final TIntDoubleHashMap propertyIDF = new TIntDoubleHashMap();

		for (TIntIntHashMap indexedDoc : results) {
			indexedDoc.forEachEntry(new TIntIntProcedure() {
				@Override
				public boolean execute(int property, int value) {
					propertyIDF.adjustOrPutValue(property, 1.0, 1.0);
					return true;
				}
			});
		}

		propertyIDF.transformValues(new TDoubleFunction() {
			@Override
			public double execute(double value) {
				
				double idf = ((double) results.size()) / (value+1);
				return idf;
			}
		});

		return propertyIDF;
	}

	private TIntObjectHashMap<TIntObjectHashMap<double[]>> getCoverageAmbiguity()
			throws Exception {

		TIntObjectHashMap<TIntObjectHashMap<double[]>> result = new TIntObjectHashMap<TIntObjectHashMap<double[]>>();

		Connection conn = kb.getConnection();

		PreparedStatement stmt = conn
				.prepareStatement(SELECT_COVERAGE_AMBIGUITY);

		for (int typeIndex : kb.getClusters()) {

			TIntObjectHashMap<double[]> propertyValues = new TIntObjectHashMap<double[]>();
			stmt.setInt(1, typeIndex);

			ResultSet rs = stmt.executeQuery();
			while (rs.next()) {
				int propery = rs.getInt(1);
				double coverage = rs.getDouble(2);
				double ambiguity = rs.getDouble(3);
				propertyValues.put(propery,
						new double[] { coverage, ambiguity });
			}
			rs.close();

			result.put(typeIndex, propertyValues);
		}
		stmt.close();

		return result;
	}

}

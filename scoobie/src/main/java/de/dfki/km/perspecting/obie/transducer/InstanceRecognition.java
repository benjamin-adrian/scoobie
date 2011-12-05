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

import gnu.trove.TIntHashSet;

import java.io.BufferedReader;
import java.io.Reader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import de.dfki.km.perspecting.obie.connection.KnowledgeBase;
import de.dfki.km.perspecting.obie.connection.ResultSetCallback;
import de.dfki.km.perspecting.obie.model.Document;
import de.dfki.km.perspecting.obie.model.SemanticEntity;
import de.dfki.km.perspecting.obie.model.Token;
import de.dfki.km.perspecting.obie.model.TokenSequence;
import de.dfki.km.perspecting.obie.workflow.Transducer;

public class InstanceRecognition extends Transducer {

	private final Logger log = Logger.getLogger(InstanceRecognition.class
			.getName());

	private boolean collabseSymbols = false;

	public void setCollabseSymbols(boolean collabseSymbols) {
		this.collabseSymbols = collabseSymbols;
	}

	@Override
	public void transduce(Document document, KnowledgeBase kb)
			throws Exception {

		// ensure lexicographic ordering
		final TreeMap<Integer, List<TokenSequence<SemanticEntity>>> matchingLiteralsMap = new TreeMap<Integer, List<TokenSequence<SemanticEntity>>>();
		final HashMap<String, Integer> literalKeyMap = new HashMap<String, Integer>();

		final List<TokenSequence<SemanticEntity>> matchingLiterals = new ArrayList<TokenSequence<SemanticEntity>>(
				document.getRetrievedPropertyValues());
		try {
			for (TokenSequence<SemanticEntity> matchingLiteral : matchingLiterals) {
				int literalValueIndex = matchingLiteral.getValue()
						.getLiteralValueIndex();
				literalKeyMap.put(matchingLiteral.getValue().toString(),
						literalValueIndex);
				List<TokenSequence<SemanticEntity>> list = matchingLiteralsMap
						.get(literalValueIndex);
				if (list == null) {
					list = new ArrayList<TokenSequence<SemanticEntity>>();
					matchingLiteralsMap.put(literalValueIndex, list);
				}
				list.add(matchingLiteral);
			}
			
			if (collabseSymbols) {
				collabseSubSymbols(document, kb, matchingLiteralsMap,
						literalKeyMap);
			}
			
			final Map<Integer, Set<Integer>> valuePropertyMap = new HashMap<Integer, Set<Integer>>();

			for (int literalValueIndex : matchingLiteralsMap.keySet()) {
				Set<Integer> properties = valuePropertyMap
						.get(literalValueIndex);
				if (properties == null) {
					properties = new HashSet<Integer>();
					valuePropertyMap.put(literalValueIndex, properties);
				}

				for (TokenSequence<SemanticEntity> namedEntity : matchingLiteralsMap
						.get(literalValueIndex)) {
					properties.add(namedEntity.getValue().getPropertyIndex());
				}
			}

			if (!valuePropertyMap.isEmpty()) {
				final ResultSetCallback rs = kb
						.getInstanceCandidates(valuePropertyMap);

				TIntHashSet subjects = new TIntHashSet();

				while (rs.getRs().next()) {
					int subjectIndex = rs.getRs().getInt(1);
					int propertyIndex = rs.getRs().getInt(2);
					int literalValueIndex = rs.getRs().getInt(3);
					String subjectUri = rs.getRs().getString(4);
//					System.out.println(subjectUri + " "+ kb.getLiteral(literalValueIndex) + " " + literalValueIndex);
					// literalsSubjectGraph.addEdge(edgeCount++,
					// -literalValueIndex, subjectIndex);
					subjects.add(subjectIndex);

					for (TokenSequence<SemanticEntity> match : matchingLiteralsMap
							.get(literalValueIndex)) {
//						System.out.println(match);
						if (match.getValue().getPropertyIndex() == propertyIndex) {
							for (int i = 0; i < match.getTokens().size(); i++) {
								if (i == 0) {
									match.getTokens().get(i).addSubject("B",
											match.getValue(), subjectIndex,
											subjectUri);
								} else {
									match.getTokens().get(i).addSubject("I",
											match.getValue(), subjectIndex,
											subjectUri);
								}
							}
						} else {
							
						}
					}

				}
				rs.close();

				HashMap<Integer, List<TokenSequence<SemanticEntity>>> subjectsLiterals = new HashMap<Integer, List<TokenSequence<SemanticEntity>>>();

				for (TokenSequence<SemanticEntity> t : document
						.getResolvedSubjects()) {

					List<TokenSequence<SemanticEntity>> list = subjectsLiterals
							.get(t.getValue().getSubjectIndex());
					if (list == null) {
						list = new ArrayList<TokenSequence<SemanticEntity>>();
						subjectsLiterals.put(t.getValue().getSubjectIndex(),
								list);
					}
					list.add(t);

				}

				addTypeToSubjects(kb, subjects.toArray(), subjectsLiterals);

			}
		} catch (Exception e) {
			log.log(Level.WARNING, e.getMessage(), e);
			throw e;
		}

	}

	/**
	 * Should be depracated, problem is already handled by RDFLiteralSpotting.
	 * 
	 * @param record
	 * @param ontology
	 * @param matchingLiteralsMap
	 * @param literalKeyMap
	 */
	private void collabseSubSymbols(
			final Document record,
			final KnowledgeBase ontology,
			final Map<Integer, List<TokenSequence<SemanticEntity>>> matchingLiteralsMap,
			final Map<String, Integer> literalKeyMap) {
		for (List<TokenSequence<SemanticEntity>> matches : new ArrayList<List<TokenSequence<SemanticEntity>>>(
				matchingLiteralsMap.values())) {
			TokenSequence<SemanticEntity> match = matches.get(0);
			if (match.getTokens().size() > 1) {
				for (Token t : match.getTokens()) {
					Integer k = literalKeyMap.get(t.toString());
					if (k != null
							&& k != match.getValue().getLiteralValueIndex()) {
						log.info("Removing subsymbol: "
										+ matchingLiteralsMap.remove(k));
					}
				}
				continue;
			}
		}
	}

	/**
	 * @param ontology
	 * @param subjects
	 * @param subjectsLiterals
	 * @throws Exception
	 * @throws SQLException
	 */
	private void addTypeToSubjects(
			final KnowledgeBase ontology,
			int[] subjects,
			HashMap<Integer, List<TokenSequence<SemanticEntity>>> subjectsLiterals)
			throws Exception, SQLException {
		final ResultSetCallback typeResults = ontology
				.getRDFTypesForInstances(subjects);

		HashMap<Integer, TIntHashSet> types = new HashMap<Integer, TIntHashSet>();

		while (typeResults.getRs().next()) {
			int subjectIndex = typeResults.getRs().getInt(1);
			int typeIndex = typeResults.getRs().getInt(2);

			TIntHashSet set = types.get(subjectIndex);
			if (set == null) {
				set = new TIntHashSet();
				types.put(subjectIndex, set);
			}
			set.add(typeIndex);
		}
		typeResults.close();

		for (int subjectIndex : types.keySet()) {
			for (TokenSequence<SemanticEntity> match : subjectsLiterals
					.get(subjectIndex)) {
				if (match.getValue().getSubjectIndex() == subjectIndex) {

					
					for (int typeIndex : types.get(subjectIndex).toArray()) {
						for(Token t : match.getTokens()) {
							t.addTypes(match.getValue(), types.get(subjectIndex).toArray(), 1.0);
						}
						match.getValue().addTypeIndex(typeIndex, 1.0);
					}
				}

			}
		}
	}

	/**
	 * Input data in line based form:
	 * 
	 * subject \n
	 * 
	 * @return
	 */

	@Override
	public String compare(Document document, KnowledgeBase kb, Reader gt)
			throws Exception {

		BufferedReader br = new BufferedReader(gt);

		HashSet<String> gtUris = new HashSet<String>();

		for (String line = br.readLine(); line != null; line = br.readLine()) {
			gtUris.add(line);
		}

		double foundRelevant = 0;
		double foundNotRelevant = 0;

		HashSet<String> foundUris = new HashSet<String>();
		for (TokenSequence<SemanticEntity> semEnt : document
				.getResolvedSubjects()) {
			String foundURI = semEnt.getValue().getSubjectURI();
			if (!foundUris.contains(foundURI)) {
				if (gtUris.contains(foundURI)) {
					foundRelevant++;
				} else {
					foundNotRelevant++;
				}
				foundUris.add(foundURI);
			}
		}

		double precision = foundRelevant / (foundRelevant + foundNotRelevant);
		double recall = foundRelevant / gtUris.size();

		if(Double.isNaN(precision)) precision = 0;
		if(Double.isNaN(recall)) recall = 0;
		
		HashSet<String> missedUris = new HashSet<String>(gtUris);
		missedUris.removeAll(foundUris);

		HashSet<String> falseUris = new HashSet<String>(foundUris);
		falseUris.removeAll(gtUris);

		String[] doc = document.getUri().toString().split("/");
		return String
				.format(
						"uri\t%s\tP\t%1.5f\tR\t%1.5f\t|GT|\t%s\t|FOUND|\t%s\tp*gt\t%1.5f\tr*gt\t%1.5f\n",
//						doc[doc.length - 1], precision, recall, gtUris.size(),foundUris.size(), missedUris.toString(), falseUris.toString()
						doc[doc.length - 1], precision, recall, gtUris.size(),foundUris.size(), precision * gtUris.size(), recall * gtUris.size()
				);
	}

}

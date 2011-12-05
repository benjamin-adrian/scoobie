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

import gnu.trove.TIntHashSet;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.Map.Entry;
import java.util.logging.Logger;

import cern.colt.matrix.DoubleMatrix1D;
import cern.colt.matrix.DoubleMatrix2D;
import cern.colt.matrix.doublealgo.Statistic;
import de.dfki.km.perspecting.obie.connection.KnowledgeBase;
import de.dfki.km.perspecting.obie.connection.ResultSetCallback;
import de.dfki.km.perspecting.obie.model.DoubleMatrix;

public class RDFTypeClustering {

	private final Logger log = Logger.getLogger(RDFTypeClustering.class
			.getName());

	private final int samples;
	private final KnowledgeBase kb;
	private double biasThreshold, pruningThreshold;

	public RDFTypeClustering(KnowledgeBase kb, int samples, double biasThreshold, double pruningThreshold) {
		this.kb = kb;
		this.samples = samples;
		this.biasThreshold = biasThreshold;
		this.pruningThreshold = pruningThreshold;
	}

	/**
	 * @param kb
	 * @return
	 * @throws Exception
	 * @throws SQLException
	 */
	public DoubleMatrix getTypeCorrelations() throws Exception {
		Set<Integer> types = new HashSet<Integer>();
		ResultSetCallback rs = kb.getRDFTypes();
		while (rs.getRs().next()) {
			types.add(rs.getRs().getInt(1));
		}
		rs.close();
		log.info("Retrieved " + types.size() + " classes");

		TIntHashSet instances = new TIntHashSet();
		final DoubleMatrix data = new DoubleMatrix();
		for (int type : types) {
			ResultSetCallback rs1 = kb.getInstancesOfTypes(type, samples);
			while (rs1.getRs().next()) {
				instances.add(rs1.getRs().getInt(1));
			}
			rs1.close();
		}
		log.info("Calculated " + samples + " samples for " + types.size()
						+ " classes");
		HashMap<Integer, Set<Integer>> typeMap = new HashMap<Integer, Set<Integer>>();

		ResultSetCallback rs2 = kb.getRDFTypesForInstances(instances.toArray());
		while (rs2.getRs().next()) {

			Set<Integer> typeSet = typeMap.get(rs2.getRs().getInt(1));
			if (typeSet == null) {
				typeSet = new HashSet<Integer>();
				typeMap.put(rs2.getRs().getInt(1), typeSet);
			}
			typeSet.add(rs2.getRs().getInt(2));
		}
		rs2.close();

		for (Entry<Integer, Set<Integer>> e : typeMap.entrySet()) {
			for (int i1 : e.getValue()) {
				for (int i2 : e.getValue()) {
					double d = data.get(i1, i2);
					data.add(i1, i2, d + 1.0);
				}
			}
		}
		log.info("Populated matrix with cooccuring types of sample instances");

		return data;
	}



	public void train() throws Exception {
		final DoubleMatrix data = getTypeCorrelations();
		log.info("Calculating covariances");
		final DoubleMatrix2D cov = data.covarianceMatrix();
		log.info("Calculating correlations");
		final DoubleMatrix2D cor = Statistic.correlation(cov);
		log.info("Start hierarchical clustering");
		DoubleMatrix2D hMatrix = data.hierarchicalLabeledClustering(cor, biasThreshold, pruningThreshold);

		resetDatabase();

		Connection conn = kb.getConnection();
		conn.setAutoCommit(false);

		double maxValue = 0;
		int bestLabel = 0;
		TIntHashSet clusterValues;
		
		TIntHashSet globalClusteredValues = new TIntHashSet();
		
		for (int row = 0; row < hMatrix.rows(); row++) {
			DoubleMatrix1D projectedColumn = hMatrix.viewRow(row);

			clusterValues = new TIntHashSet();
			maxValue = -1;
			bestLabel = -1;

			for (int col = 0; col < hMatrix.columns(); col++) {
				if (projectedColumn.get(col) > 0) {
					clusterValues.add(data.getColKeys()[col]);
					globalClusteredValues.add(data.getColKeys()[col]);
					if (projectedColumn.get(col) > maxValue) {
						maxValue = projectedColumn.get(col);
						bestLabel = data.getColKeys()[col];
					}
				}
			}

			for (int type : clusterValues.toArray()) {
				conn.createStatement().executeUpdate(
						"INSERT INTO type_clusters VALUES (" + type + ", "
								+ bestLabel + ")");
				log.info("Clustering " + kb.getURI(type) + " as " + kb.getURI(bestLabel));
			}
		}
		
		ResultSetCallback rs = kb.getRDFTypes();
		while(rs.getRs().next()) {
			int type = rs.getRs().getInt(1);
			if(!globalClusteredValues.contains(type)) {
				conn.createStatement().executeUpdate(
						"INSERT INTO type_clusters VALUES (" + type + ", "
								+ type + ")");
				log.info("Clustering " + kb.getURI(type) + " as " + kb.getURI(type));
			}
		}
		rs.close();

		conn.commit();
		conn.setAutoCommit(true);
	}

	private void resetDatabase() throws SQLException {

		Connection conn = kb.getConnection();
		conn.setAutoCommit(false);
		String drop = "DROP TABLE IF EXISTS type_clusters CASCADE;";
		String create = "CREATE TABLE type_clusters (type int, cluster int)";

		conn.createStatement().executeUpdate(drop);
		conn.createStatement().executeUpdate(create);
		conn.commit();
		conn.setAutoCommit(true);
	}

}

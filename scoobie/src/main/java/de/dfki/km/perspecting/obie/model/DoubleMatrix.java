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

import gnu.trove.TIntDoubleHashMap;
import gnu.trove.TIntHashSet;
import gnu.trove.TIntObjectHashMap;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Formatter;
import java.util.HashSet;
import java.util.List;
import java.util.TreeSet;
import java.util.logging.Logger;

import cern.colt.function.DoubleFunction;
import cern.colt.list.DoubleArrayList;
import cern.colt.matrix.DoubleMatrix1D;
import cern.colt.matrix.DoubleMatrix2D;
import cern.colt.matrix.doublealgo.Statistic;
import cern.colt.matrix.impl.DenseDoubleMatrix2D;
import cern.colt.matrix.impl.SparseDoubleMatrix2D;
import cern.colt.matrix.linalg.Algebra;
import cern.colt.matrix.linalg.SingularValueDecomposition;
import cern.jet.stat.Descriptive;
import cern.jet.stat.Probability;
import de.dfki.km.perspecting.obie.connection.KnowledgeBase;
import de.dfki.km.perspecting.obie.preprocessor.HierarchicalClustering;
import de.dfki.km.perspecting.obie.preprocessor.HierarchicalClustering.Cluster;
import de.dfki.km.perspecting.obie.preprocessor.HierarchicalClustering.Leaf;
import de.dfki.km.perspecting.obie.preprocessor.HierarchicalClustering.Traversal;

/**
 * 
 * This data sheet summarizes emerging hypotheses raised by extraction tasks.
 * 
 * @author adrian
 * 
 */
public class DoubleMatrix {

	private final Logger log = Logger.getLogger(DoubleMatrix.class.getName());

	private final TIntObjectHashMap<TIntDoubleHashMap> matrix = new TIntObjectHashMap<TIntDoubleHashMap>();

	private final TreeSet<Integer> rowKeys = new TreeSet<Integer>();
	private final TreeSet<Integer> colKeys = new TreeSet<Integer>();

	public Integer[] getRowKeys() {
		return rowKeys.toArray(new Integer[rowKeys.size()]);
	}

	public Integer[] getColKeys() {
		return colKeys.toArray(new Integer[colKeys.size()]);
	}

	public void add(int colKey, int rowKey, double value) {
		if (!matrix.containsKey(colKey)) {
			matrix.put(colKey, new TIntDoubleHashMap());
		}
		TIntDoubleHashMap col = matrix.get(colKey);
		col.put(rowKey, value);

		rowKeys.add(rowKey);
		colKeys.add(colKey);
	}

	public double get(int colKey, int rowKey) {
		TIntDoubleHashMap col = matrix.get(colKey);
		if (col != null) {
			return col.get(rowKey);
		} else {
			return 0;
		}
	}

	public double[] getColumn(int col) {

		double[] colVector = new double[getRowKeys().length];

		int i = 0;
		for (int row : getRowKeys()) {
			colVector[i++] = matrix.get(col).get(row);
		}
		return colVector;
	}

	public double[] getRow(int row) {
		double[] rowVector = new double[getColKeys().length];
		int i = 0;
		for (int col : getColKeys()) {
			rowVector[i++] = matrix.get(col).get(row);
		}
		return rowVector;
	}

	public double averagePrecision(int[] gt, int column) {

		Integer[] keys = getRowKeys();
		double[] ratings = getColumn(column);

		List<TIntDoubleTuple> ranking = new ArrayList<TIntDoubleTuple>();

		for (int i = 0; i < keys.length; i++) {
			ranking.add(new TIntDoubleTuple(keys[i], ratings[i]));
		}

		Collections.sort(ranking, new Comparator<TIntDoubleTuple>() {
			@Override
			public int compare(TIntDoubleTuple o1, TIntDoubleTuple o2) {
				return Double.compare(o2.value, o1.value);
			}
		});

		TIntHashSet gtSet = new TIntHashSet(gt);

		double sum = 0.0;
		double rank = 1.0;
		double foundRelevantEntries = 0;
		for (TIntDoubleTuple k : ranking) {
			if (gtSet.contains(k.key)) {
				foundRelevantEntries++;
				sum += (foundRelevantEntries / rank);
			}
			rank++;
		}

		double ap = sum / gt.length;

		return ap;
	}

	public double recall(int[] gt, int column) {

		Integer[] found = getRowKeys();
		TIntHashSet relevant = new TIntHashSet(gt);

		double foundRelevant = 0;

		for (int k : found) {
			if (relevant.contains(k)) {
				foundRelevant++;
			}
		}

		double p = foundRelevant / relevant.size();
		return p;
	}

	public double precision(int[] gt, int column) {

		Integer[] found = getRowKeys();
		TIntHashSet gtSet = new TIntHashSet(gt);

		double foundRelevant = 0;

		for (int k : found) {
			if (gtSet.contains(k)) {
				foundRelevant++;
			}
		}

		double p = foundRelevant / found.length;
		return p;
	}

	public double mean(int column) {
		return mean(getColumn(column));
	}

	private double mean(double[] v) {
		double sum = 0;

		for (double d : v) {
			sum += d;
		}

		return sum / v.length;
	}

	public double probability(int col) {

		double sum = 0;

		for (int row : getRowKeys()) {
			sum += get(col, row);
		}
		return sum / rowKeys.size();
	}

	public DoubleMatrix2D cosineSimilarity(int...rows) {
		DoubleMatrix2D origin = Algebra.DEFAULT.transpose(toColt());
		DoubleMatrix2D similarities = new DenseDoubleMatrix2D(origin.columns(),
				origin.columns());

		for (int row : rows) {
			DoubleMatrix1D r = origin.viewRow(row);
			for (int i = 0; i < origin.columns(); i++) {
				similarities.set(i, i, 1.0);
				if (r.get(i) > 0) {
					for (int j = i + 1; j < origin.columns(); j++) {
						DoubleMatrix1D r1 = origin.viewRow(row);
						if (r1.get(j) > 0) {
							DoubleMatrix1D a = origin.viewColumn(i);
							DoubleMatrix1D b = origin.viewColumn(j);

							double cosineDistance = a.zDotProduct(b)
									/ Math.sqrt(a.zDotProduct(a)
											* b.zDotProduct(b));

							similarities.set(i, j, cosineDistance);
							similarities.set(j, i, cosineDistance);
						}
					}
				}
			}
		}
		return similarities;
	}

	public DoubleMatrix2D cosineSimilarity() {
		DoubleMatrix2D origin = Algebra.DEFAULT.transpose(toColt());
		DoubleMatrix2D similarities = new DenseDoubleMatrix2D(origin.columns(),
				origin.columns());

		for (int i = 0; i < origin.columns(); i++) {
			similarities.set(i, i, 1.0);
			for (int j = 0; j < origin.columns(); j++) {
				DoubleMatrix1D a = origin.viewColumn(i);
				DoubleMatrix1D b = origin.viewColumn(j);
				double cosineDistance = a.zDotProduct(b) / Math.sqrt(a.zDotProduct(a) * b.zDotProduct(b));
//				System.out.println(i +" " + j + " = " +cosineDistance);
//				System.out.println(a +"cos "+ b + " = " +cosineDistance);
				similarities.set(i, j, cosineDistance);
//				similarities.set(j, i, cosineDistance);

			}

		}

		return similarities;
	}

	/**
	 * 
	 * @param similarity
	 *            Correlation matrix between all rows.
	 * 
	 * @return
	 */
	public DoubleMatrix2D predictValuesByCosine(DoubleMatrix2D similarity,
			DoubleMatrix2D matrix) {

		final DoubleMatrix2D predictedMatrix = new SparseDoubleMatrix2D(matrix
				.rows(), matrix.columns());

		for (int row = 0; row < matrix.rows(); row++) {
			for (int col = 0; col < matrix.columns(); col++) {
				double value = matrix.get(row, col);
				if (value == 0) {
					for (int row2 = 0; row2 < matrix.rows(); row2++) {
						if (row != row2) {
							double sim = similarity.get(row, row2);
							double nValue = matrix.get(row2, col);
							double evidence = sim * nValue;
							value += evidence;
						}
					}
				}

				predictedMatrix.set(row, col, value);
			}
		}
		return predictedMatrix;
	}

	public DoubleMatrix2D predictValues(DoubleMatrix2D cor,
			DoubleMatrix2D origin) {
		final DoubleMatrix2D predictedMatrix = new SparseDoubleMatrix2D(
				getRowKeys().length, getColKeys().length);

		// compare all rows by correlation
		for (int rowA = 0; rowA < cor.rows(); rowA++) {

			int col = 0;
			for (double colValue : origin.viewRow(rowA).toArray()) {
				if (colValue == 0) { // predict if no value exists

					// double sumOfPosCorValue = 0.0;

					for (int rowB = 0; rowB < cor.rows(); rowB++) {
						if (rowB == rowA) {
							continue; // skip self correlations
						} else {
							double valueOfNeighbor = origin.get(rowB, col);
							double correlationValue = cor.get(rowA, rowB);
							double evidence = correlationValue
									* valueOfNeighbor;
							// if (evidence > 0) {
							// sumOfPosCorValue++;
							// }
							colValue *= evidence;
						}
					}

					// divide by the sum of all positive correlations
					// if (colValue > 0)
					// colValue /= sumOfPosCorValue;
					predictedMatrix.set(rowA, col, colValue);
				}
				col++;
			}
		}
		return predictedMatrix;
	}

	public double standardDeviation(int column) {
		return standardDeviation(getColumn(column));
	}

	private double standardDeviation(double[] v) {
		double mean = mean(v);
		double sum = 0.0;
		for (double d : v) {
			sum += Math.pow(d - mean, 2.0);
		}
		return Math.sqrt(sum / (v.length - 1));
	}

	public double significance(int column1, int column2, int n) {

		TIntDoubleHashMap col1 = matrix.get(column1);
		TIntDoubleHashMap col2 = matrix.get(column2);

		double k = col1.get(column2);

		double count1 = col1.get(column1);
		double count2 = col2.get(column2);

		double lambda = (count1 * count2) / n;

		double p = Probability.poissonComplemented((int) k + 1, lambda);

		if (p == 1.0) {
			p = Double.MAX_VALUE;
		} else {
			p = -Math.log(p);
		}

		return p;

	}

	public void normalize(int column) {
		TIntDoubleHashMap col = matrix.get(column);
		double mean = mean(column);
		double sd = standardDeviation(column);

		if (sd == 0) {
			sd = 1.0;
		}
		for (int row : col.keys()) {
			double d = col.get(row);
			double d1 = (((d - mean) / sd) / 2.0) + 1.0;
			
			if(d1 <= 0) {
				log.warning("recognized outlier: " + column +",  " + mean);
				d1 = 0.00001;
			}
			
			col.put(row, d1);
		}
	}

	public DoubleMatrix2D spearmanCorrelationMatrix() throws IOException {

		DoubleMatrix2D m = toColt();

		for (int col = 0; col < m.columns(); col++) {
			double[] column = m.viewColumn(col).toArray();
			double[] sorted = Arrays.copyOf(column, column.length);
			Arrays.sort(sorted);

			for (int row = 0; row < m.rows(); row++) {
				int rank = column.length - 1
						- Arrays.binarySearch(sorted, column[row]);
				m.set(row, col, rank);
			}
		}

		return Statistic.correlation(Statistic.covariance(m));
	}

	public DoubleMatrix2D pearsonCorrelationDoubleMatrix() {
		return Statistic.correlation(covarianceMatrix());
	}

	public DoubleMatrix2D covarianceMatrix() {
		return Statistic.covariance(toColt());
	}

	public DoubleMatrix2D conditionalProbabiltyDoubleMatrix(double threshold) {
		DoubleMatrix2D matrix = new SparseDoubleMatrix2D(getRowKeys().length,
				getColKeys().length);

		for (int i = 0; i < getRowKeys().length; i++) {
			for (int j = 0; j < getColKeys().length; j++) {
				double value = conditionalProbability(getRowKeys()[i],
						getColKeys()[j], threshold);
				matrix.set(i, j, value);
			}
		}
		return matrix;
	}

	public DoubleMatrix2D conditionalProbabiltyDoubleMatrixUpper(
			double threshold) throws IOException {
		DoubleMatrix2D matrix = new SparseDoubleMatrix2D(getRowKeys().length,
				getColKeys().length);

		for (int i = 0; i < getRowKeys().length; i++) {
			for (int j = 0; j <= i; j++) {
				double value = conditionalProbability(getRowKeys()[i],
						getColKeys()[j], threshold);
				matrix.set(i, j, value);
			}
		}
		return matrix;
	}

	public DoubleMatrix2D hierarchicalClustering(DoubleMatrix2D cov) {
		DoubleMatrix2D origin = toColt();
		HierarchicalClustering cl = new HierarchicalClustering();
		Cluster root = cl.cluster(cov);

		final DoubleArrayList distances = new DoubleArrayList();
		final ArrayList<Cluster> clusters = new ArrayList<Cluster>();

		root.forEachDescendent(root, new Traversal() {
			@Override
			public boolean test(Cluster cluster) {
				if (!Double.isInfinite(cluster.getDistance())) {
					distances.add(cluster.getDistance());
				}
				clusters.add(cluster);
				return true;
			}
		});

		final double threshold = Descriptive.mean(distances) * 1.5;

		final ArrayList<Cluster> collabsedClusters = new ArrayList<Cluster>();

		root.forEachDescendent(root, new Traversal() {
			@Override
			public boolean test(Cluster cluster) {

				double distance = cluster.getDistance();
				if (distance <= threshold) {
					collabsedClusters.add(cluster);
					return false;
				}
				return true;
			}
		});

		final DoubleMatrix2D clusterMatrix = new SparseDoubleMatrix2D(origin
				.rows(), origin.columns());
		int crow = 0;
		for (Cluster cluster : collabsedClusters) {
			for (int col : cluster.getValues()) {
				clusterMatrix.set(crow, col, cluster.getDistance());
			}
			crow++;
		}

		return clusterMatrix;

	}

	public DoubleMatrix1D minus(DoubleMatrix1D a, DoubleMatrix1D b) {

		DoubleMatrix1D diff = a.copy();
		for (int i = 0; i < diff.size(); i++) {
			diff.setQuick(i, diff.getQuick(i) - b.getQuick(i));
		}

		return diff;
	}

	public DoubleMatrix2D[] hierarchicalLabeledClustering(DoubleMatrix2D cov,
			double bias, double[] thresholds) {
		DoubleMatrix2D origin = toColt();
		HierarchicalClustering cl = new HierarchicalClustering();
		Cluster root = cl.cluster(cov);

		final DoubleArrayList distances = new DoubleArrayList();
		final ArrayList<Cluster> clusters = new ArrayList<Cluster>();

		root.forEachDescendent(root, new Traversal() {
			@Override
			public boolean test(Cluster cluster) {
				if (!Double.isInfinite(cluster.getDistance())) {
					distances.add(cluster.getDistance());
				}
				clusters.add(cluster);
				return true;
			}
		});
		log.info("Number of clusters: " + clusters.size());

		DoubleMatrix2D[] result = new DoubleMatrix2D[thresholds.length];

		int i = 0;
		for (double threshold : thresholds) {
			result[i++] = labelClusters(bias, origin, extractClusters(root,
					distances, Descriptive.max(distances) * threshold));
		}
		return result;
	}

	/**
	 * @param root
	 * @param distances
	 * @param threshold
	 * @return
	 */
	private ArrayList<Cluster> extractClusters(Cluster root,
			final DoubleArrayList distances, final double threshold) {

		log.info("Max: " + Descriptive.max(distances));
		log.info("Threshold: " + threshold);

		final ArrayList<Cluster> collabsedClusters = new ArrayList<Cluster>();

		root.forEachDescendent(root, new Traversal() {
			@Override
			public boolean test(Cluster cluster) {

				double distance = cluster.getDistance();
				if (distance <= threshold) {
					collabsedClusters.add(cluster);
					return false;
				} else if (cluster instanceof Leaf) {
					collabsedClusters.add(cluster);
					return false;
				}

				return true;
			}
		});

		return collabsedClusters;
	}

	/**
	 * @param bias
	 * @param origin
	 * @param collabsedClusters
	 * @return
	 */
	private DoubleMatrix2D labelClusters(double bias, DoubleMatrix2D origin,
			final ArrayList<Cluster> collabsedClusters) {
		final DoubleMatrix2D clusterMatrix = new SparseDoubleMatrix2D(origin
				.rows(), origin.columns());

		// ScoobieLogging.log("", ScoobieLogging.TRAINING,
		// "Calculating correlations", log);
		final DoubleMatrix2D cp = conditionalProbabiltyDoubleMatrix(bias);
		//
		// ScoobieLogging.log("", ScoobieLogging.TRAINING,
		// "Calculating cluster labels", log);
		int crow = 0;
		for (Cluster cluster : collabsedClusters) {

			final TIntDoubleHashMap ancestorsMap = new TIntDoubleHashMap();
			final TIntDoubleHashMap distanceMap = new TIntDoubleHashMap();

			Integer[] clusterValues = cluster.getValues();

			HashSet<Integer> clusterValueSet = new HashSet<Integer>(Arrays
					.asList(clusterValues));

			for (int row = 0; row < cp.columns(); row++) {
				for (int col : clusterValues) {
					double row_if_col = cp.get(row, col);
					double col_if_row = cp.get(col, row);

					if (row_if_col * col_if_row > 0) {

						ancestorsMap.adjustOrPutValue(row, row_if_col
								/ clusterValues.length, row_if_col
								/ clusterValues.length);

						distanceMap.adjustOrPutValue(row, col_if_row
								/ clusterValues.length, col_if_row
								/ clusterValues.length);
					}

				}
			}

			HashSet<Integer> outerclusterValueSet = new HashSet<Integer>();

			double internalMaxValue = 0;
			for (int k : ancestorsMap.keys()) {
				double v = ancestorsMap.get(k) * distanceMap.get(k);
				if (!clusterValueSet.contains(k)) {
					outerclusterValueSet.add(k);
				} else {
					if (v > internalMaxValue) {
						internalMaxValue = v;
					}
				}
				clusterMatrix.set(crow, k, v);
			}

			double externalMaxValue = 0;
			Integer maxK = -1;

			for (int k : outerclusterValueSet) {
				if (clusterMatrix.get(crow, k) > externalMaxValue) {
					maxK = k;
					externalMaxValue = clusterMatrix.get(crow, k);
				}
			}

			if (internalMaxValue < externalMaxValue)
				outerclusterValueSet.remove(maxK);

			for (int k : outerclusterValueSet) {
				clusterMatrix.set(crow, k, 0);
			}

			crow++;
		}

		return clusterMatrix;
	}

	public DoubleMatrix2D hierarchicalLabeledClustering(DoubleMatrix2D cov,
			double bias, double l_threshold) {
		DoubleMatrix2D origin = toColt();
		HierarchicalClustering cl = new HierarchicalClustering();
		Cluster root = cl.cluster(cov);

		final DoubleArrayList distances = new DoubleArrayList();
		final ArrayList<Cluster> clusters = new ArrayList<Cluster>();

		root.forEachDescendent(root, new Traversal() {
			@Override
			public boolean test(Cluster cluster) {
				if (!Double.isInfinite(cluster.getDistance())) {
					distances.add(cluster.getDistance());
				}
				clusters.add(cluster);
				return true;
			}
		});

		final double threshold = Descriptive.max(distances) * l_threshold;// Descriptive.mean(distances)
		// *
		// 1.5;

		log.info("Max: " + Descriptive.max(distances));
		log.info("Mean: " + threshold);

		final ArrayList<Cluster> collabsedClusters = new ArrayList<Cluster>();

		root.forEachDescendent(root, new Traversal() {
			@Override
			public boolean test(Cluster cluster) {

				double distance = cluster.getDistance();
				if (distance <= threshold) {
					collabsedClusters.add(cluster);
					return false;
				}
				return true;
			}
		});

		return labelClusters(bias, origin, collabsedClusters);

	}

	public DoubleMatrix2D pca(double percentage, DoubleMatrix2D cov) {
		DoubleMatrix2D origin = toColt();
		final SingularValueDecomposition svd = new SingularValueDecomposition(
				cov);
		double cummulativeEnergy = 0;

		for (double eigenValue : svd.getSingularValues()) {
			cummulativeEnergy += eigenValue;
		}

		// log.info("Energy: " + cummulativeEnergy);
		double threshold = cummulativeEnergy * percentage;
		// log.info("Threshold: " + threshold);
		double _cummulativeEnergy = 0;
		int count = 0;
		for (double eigenValue : svd.getSingularValues()) {
			_cummulativeEnergy += eigenValue;
			count++;
			if (_cummulativeEnergy >= threshold) {
				break;
			}
		}
		// log.info(count + ": " + _cummulativeEnergy);
		DoubleMatrix2D pc = svd.getU();

		DoubleMatrix2D rpc = pc.viewPart(0, 0, getColKeys().length, count);

		DoubleMatrix2D collabsedMatrix = origin.zMult(rpc,
				new DenseDoubleMatrix2D(getRowKeys().length, count));

		for (int col = 0; col < collabsedMatrix.columns(); col++) {
			DoubleMatrix1D columnVector = collabsedMatrix.viewColumn(col);

			final double mean = Descriptive.mean(new DoubleArrayList(
					columnVector.toArray()));
			columnVector.assign(new DoubleFunction() {
				public double apply(double argument) {
					return argument - mean;
				}
			});
		}

		for (int col = 0; col < collabsedMatrix.columns(); col++) {
			DoubleMatrix1D columnVector = collabsedMatrix.viewColumn(col);
			final double stdMean = Math.sqrt(svd.getSingularValues()[col]);
			columnVector.assign(new DoubleFunction() {
				public double apply(double argument) {
					return argument / stdMean;
				}
			});
		}

		return collabsedMatrix;

	}

	/**
	 * @return
	 */
	public DoubleMatrix2D toColt() {

		DoubleMatrix2D coltMatrix = new SparseDoubleMatrix2D(
				getRowKeys().length, getColKeys().length);
		for (int i = 0; i < coltMatrix.rows(); i++) {
			for (int j = 0; j < coltMatrix.columns(); j++) {
				coltMatrix.set(i, j, get(getColKeys()[j], getRowKeys()[i]));
			}
		}
		return coltMatrix;
	}

	public double[][] toArray() {

		double[][] matrix = new double[getRowKeys().length][getColKeys().length];
		for (int i = 0; i < getRowKeys().length; i++) {
			for (int j = 0; j < getColKeys().length; j++) {
				matrix[i][j] = get(getColKeys()[j], getRowKeys()[i]);
			}
		}

		return matrix;
	}

	public void serialize(FileWriter writer, KnowledgeBase kb) throws Exception {

		Integer[] columns = getColKeys();

		Formatter f = new Formatter(writer);
		writer.append('\t');
		for (int i = 0; i < columns.length; i++) {
			writer.append("\"" + kb.getURI(columns[i]) + "\"");
			if (i < columns.length - 1) {
				writer.append('\t');
			} else {
				writer.append('\n');
			}
		}

		for (int i = 0; i < columns.length; i++) {
			writer.append("\"" + kb.getURI(columns[i]) + "\"");
			writer.append('\t');
			for (int j = 0; j < columns.length; j++) {
				{
					double value = get(columns[i], columns[j]);
					f.format("%1.2f", (Double) value).flush();
					if (j < columns.length - 1) {
						writer.append('\t');
					} else {
						writer.append('\n');
					}
				}
			}
		}

		f.flush();
	}

	public double conditionalProbability(int a, int b, double threshold) {

		double sumB = get(b, b);

		double bInA = get(a, b);
		double aInB = get(b, a);

		if (sumB >= threshold && bInA >= threshold && aInB >= threshold) {
			return aInB / sumB;
		} else {
			return 0.0;
		}
	}

	public void fuseRatings(int title, Skalar skalar, int ... columns) {
		// title = Arrays.toString(columns) + "_" + skalar.toString();

		Arrays.sort(columns);
		TIntHashSet keys = new TIntHashSet();
		
		if (columns.length > 0) {
			keys.addAll(matrix.get(columns[0]).keys());

			for (int i = 1; i < columns.length; i++) {
				keys.retainAll(matrix.get(columns[i]).keys());
			}

			for (int key : keys.toArray()) {
				double[] values = new double[columns.length];
				for (int i = 0; i < columns.length; i++) {
					values[i] = matrix.get(columns[i]).get(key);
				}

				add(title, key, skalar.skalar(values));
			}
		}

	}

	public interface Skalar {
		double skalar(double[] values);
	}

	public static Skalar BELIEF_AGGREGRATION = new Skalar() {
		public double skalar(double[] values) {

			double belief = values[0];

			for (int i = 1; i < values.length; i++) {
				belief = belief + (1.0 - belief) * values[i];
			}
			return belief;
		}

		public String toString() {
			return "BELIEF_AGGREGRATION";
		}
	};

	public static Skalar PRODUCT = new Skalar() {
		public double skalar(double[] values) {

			double belief = 1.0;

			for (int i = 0; i < values.length; i++) {
				if (values[i] != 0)
					belief *= values[i];
			}
			return belief;
		}

		public String toString() {
			return "PRODUCT";
		}
	};

}

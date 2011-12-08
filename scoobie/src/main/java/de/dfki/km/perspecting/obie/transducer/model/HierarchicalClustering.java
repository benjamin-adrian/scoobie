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

package de.dfki.km.perspecting.obie.transducer.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import cern.colt.function.DoubleFunction;
import cern.colt.matrix.DoubleMatrix1D;
import cern.colt.matrix.DoubleMatrix2D;
import cern.colt.matrix.impl.DenseDoubleMatrix1D;

public class HierarchicalClustering {

	public Cluster cluster(final DoubleMatrix2D matrix) {

		final List<Cluster> clusterMatrix = new ArrayList<Cluster>();
		for (int i = 0; i < matrix.rows(); i++) {
			clusterMatrix.add(new Leaf(i, matrix.rows()));
		}

		Cluster root = null;
		while (clusterMatrix.size() > 1) {
			root = getClosestClusters(clusterMatrix, matrix);
		}

		return root;
	}

	private ArrayList<DoubleMatrix1D> vectors = new ArrayList<DoubleMatrix1D>();
	
	public ArrayList<DoubleMatrix1D> getVectors() {
		return vectors;
	}
	
	private Cluster getClosestClusters(final List<Cluster> clusters,
			final DoubleMatrix2D matrix) {

		final Cluster c = new Cluster();

		int minA = 0, minB = 0;

		for (int a = 0; a < clusters.size(); a++) {
			for (int b = 0; b < a; b++) {
				if (a != b) {
					double distance = getAVGDistance(clusters.get(a), clusters
							.get(b), matrix);
					if (distance < c.distance) {
						c.distance = distance;
						c.childA = clusters.get(a);
						c.childB = clusters.get(b);
						minA = a;
						minB = b;
					}
				}
			}
		}

		if (minA < minB) {
			clusters.remove(minB);
			clusters.remove(minA);
		} else {
			clusters.remove(minA);
			clusters.remove(minB);
		}

		c.childA.father = c;
		c.childB.father = c;
		clusters.add(c);
		vectors.add(c.childA.toAVGVector(matrix));
		
		c.level = clusters.size();
		return c;
	}

	private double getMinDistance(final Cluster clusterA,
			final Cluster clusterB, final double[][] distanceMatrix) {
		double d = Double.MAX_VALUE;

		for (int a : clusterA.getValues()) {
			for (int b : clusterB.getValues()) {
				double distance = euclideanDistance(distanceMatrix[a],
						distanceMatrix[b]);
				d = d > distance ? distance : d;
			}
		}
		return d;
	}

	private double getAVGDistance(final Cluster clusterA,
			final Cluster clusterB, final DoubleMatrix2D distanceMatrix) {
		double d = 0.0;

		Integer[] av = clusterA.getValues();
		Integer[] bv = clusterB.getValues();

		for (int a : av) {
			for (int b : bv) {
				double distance = euclideanDistance(distanceMatrix.viewRow(a)
						.toArray(), distanceMatrix.viewRow(b).toArray());
				d += distance;
			}
		}
		return d / (av.length * bv.length);
	}

	private double euclideanDistance(final double[] a, final double[] b) {

		double distance = 0.0;

		for (int i = 0; i < a.length; i++) {
			distance += Math.pow(a[i] - b[i], 2.0);
		}

		return Math.sqrt(distance);
	}
	

	public class Cluster {
		double distance = Double.POSITIVE_INFINITY;

		int level;

		Cluster father;

		int label;
		Cluster childA;
		Cluster childB;

		Cluster[] getChildren() {
			return new Cluster[] { childA, childB };
		}

		public void setLabel(int label) {
			this.label = label;
		}

		public int getLabel() {
			return label;
		}

		public int getLevel() {
			return level;
		}

		public double getDistance() {
			return distance;
		}

		public Cluster getFather() {
			return father;
		}
		
		public DoubleMatrix1D toAVGVector(final DoubleMatrix2D distanceMatrix) {
			DoubleMatrix1D sum = new DenseDoubleMatrix1D(distanceMatrix.columns());
			Integer[] av = getValues();
			for (int a : av) {
				DoubleMatrix1D row = distanceMatrix.viewRow(a);
				
				for (int i = 0 ; i < row.size(); i++) {
					sum.setQuick(i, sum.getQuick(i) + row.get(i));
				}
			}
			
			sum.assign(new DoubleFunction() {
				@Override
				public double apply(double argument) {
					return argument / distanceMatrix.columns();
				}
			});
			
			return sum;

		}


		public Integer[] getValues() {
			ArrayList<Integer> values = new ArrayList<Integer>();
			traverseValues(this, values);
			return values.toArray(new Integer[values.size()]);
		}

		void traverseValues(Cluster anchor, ArrayList<Integer> values) {

			for (Cluster child : anchor.getChildren()) {
				child.traverseValues(child, values);
			}
		}

		public void forEachDescendent(Cluster anchor, Traversal function) {
			if (function.test(anchor)) {
				for (Cluster child : anchor.getChildren()) {
					child.forEachDescendent(child, function);
				}
			}
		}

		public String toString(String[] labels) {
			return String.format("%.2f", distance) + ":"
					+ Arrays.toString(resolveLabels(getValues(), labels));
		}

		@Override
		public String toString() {
			return String.format("%.2f", distance) + ":"
					+ Arrays.toString(getValues());
		}

		private String[] resolveLabels(Integer[] indexes, String[] labels) {
			String[] out = new String[indexes.length];
			for (int i = 0; i < indexes.length; i++) {
				out[i] = (labels == null) ? indexes[i].toString()
						: labels[indexes[i]];
			}
			return out;
		}
	}

	public final class Leaf extends Cluster {
		int value;

		Leaf(int value, int level) {
			this.value = value;
			this.level = level;
		}

		@Override
		Cluster[] getChildren() {
			return new Cluster[] {};
		}

		@Override
		public Integer[] getValues() {
			return new Integer[] { value };
		}

		@Override
		void traverseValues(Cluster anchor, ArrayList<Integer> values) {
			values.add(value);
		}

		@Override
		public String toString() {
			return String.format("%.2f", distance) + ":" + value;
		}

		@Override
		public String toString(String[] labels) {
			return String.format("%.2f", distance) + ":" + labels[value];
		}
	}

	public interface Traversal {
		abstract boolean test(Cluster cluster);
	}
}
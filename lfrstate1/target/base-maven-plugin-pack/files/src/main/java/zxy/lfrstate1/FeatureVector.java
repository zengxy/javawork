package zxy.lfrstate1;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.lang.StringUtils;

/**
 * @author zxy
 * @category 特征向量类，提供初始化、解析等操作
 */
public class FeatureVector {
	private List<Double> featureList;
	private int featureLen;

	public FeatureVector(int featureLen) {
		this.featureLen = featureLen;
		this.featureList = new ArrayList<Double>();
		int i = featureLen;
		Random random = new Random();
		while (i > 0) {
			i--;
			featureList.add(random.nextGaussian());
		}
	}

	public FeatureVector(int featureLen, Double initDouble) {
		this.featureLen = featureLen;
		this.featureList = new ArrayList<Double>();
		int i = featureLen;
		while (i > 0) {
			i--;
			featureList.add(initDouble);
		}
	}

	public FeatureVector(String featureString) {
		String[] features = featureString.split(",");
		this.featureList = new ArrayList<Double>();
		this.featureLen = features.length;
		for (int i = 0; i < features.length; i++) {
			featureList.add(Double.parseDouble(features[i]));
		}
	}

	public FeatureVector(FeatureVector f) {
		this.featureList = new ArrayList<Double>();
		this.featureList.addAll(f.featureList);
		this.featureLen = this.featureList.size();
	}

	public static double innerProduct(FeatureVector f1, FeatureVector f2) {
		if (f1.featureLen != f2.featureLen)
			return Double.NaN;
		else {
			Double product = 0.0;
			for (int i = 0; i < f1.featureLen; i++) {
				product += f1.featureList.get(i) * f2.featureList.get(i);
			}
			return product;
		}
	}

	/**
	 * 自增
	 */
	public void selfIncrease(FeatureVector f) {
		for (int i = 0; i < this.featureLen; i++)
			this.featureList.set(i,
					this.featureList.get(i) + f.featureList.get(i));
	}

	/**
	 * 自减
	 */
	public void selfDecrease(FeatureVector f) {
		for (int i = 0; i < this.featureLen; i++)
			this.featureList.set(i,
					this.featureList.get(i) - f.featureList.get(i));
	}

	/**
	 * 向量加法
	 * 
	 * @param f1加数
	 * @param f2加数
	 * @return 返回值为新的FeatureVector向量，不改变f1，f2
	 */
	public static FeatureVector featureAdd(FeatureVector f1, FeatureVector f2) {
		FeatureVector returnFeatureVector = new FeatureVector(f1);
		for (int i = 0; i < f1.featureLen; i++) {
			returnFeatureVector.featureList.set(
					i,
					returnFeatureVector.featureList.get(i)
							+ f2.featureList.get(i));
		}

		return returnFeatureVector;
	}

	/**
	 * 向量减法
	 * 
	 * @param f1减数
	 * @param f2被减数
	 * @return 返回值为新的FeatureVector向量，不改变f1，f2
	 */
	public static FeatureVector featureSub(FeatureVector f1, FeatureVector f2) {
		FeatureVector returnFeatureVector = new FeatureVector(f1);
		for (int i = 0; i < f1.featureLen; i++) {
			returnFeatureVector.featureList.set(
					i,
					returnFeatureVector.featureList.get(i)
							- f2.featureList.get(i));
		}

		return returnFeatureVector;

	}

	/**
	 * 向量乘数，改变原向量
	 * 
	 * @param multiplier
	 */
	public void featureMultiply(double multiplier) {
		for (int i = 0; i < this.featureLen; i++)
			this.featureList.set(i, this.featureList.get(i) * multiplier);
	}

	public String getFeatureString() {
		return StringUtils.join(this.featureList, ",");
	}

}

package zxy.utilitytest;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.lang.StringUtils;

public class test {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		FeatureVector f1=new FeatureVector(2);
		FeatureVector f2=new FeatureVector(2);
		
		System.out.println(f1.getFeatureString());
		System.out.println(f2.getFeatureString());
		
		FeatureVector f3=FeatureVector.featureAdd(f1, f2);
		
		System.out.println(f1.getFeatureString());
		System.out.println(f2.getFeatureString());
		System.out.println(f3.getFeatureString());
		f3.featureMultiply(2);
		System.out.println(f3.getFeatureString());
	}

}

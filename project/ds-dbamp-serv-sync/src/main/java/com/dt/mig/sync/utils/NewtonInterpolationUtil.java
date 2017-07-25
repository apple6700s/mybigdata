package com.dt.mig.sync.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by abel.chan on 17/4/6.
 */
public class NewtonInterpolationUtil {
    /*拷贝向量*/
    private static void copy_vector(double from[], double to[]) {
        int k = from.length;
        int k2 = to.length;
        if (k != k2) {
            System.out.println("the two vector's length is not equal!");
            System.exit(0);
        }
        for (int i = 0; i < k; i++) {
            to[i] = from[i];
        }

    }

    /*牛顿插值法*/
    public static List<Integer> Newton_inter_method(double[] X, double[] Y, double X0[], int total) {
        if (total <= 0) {
            return Arrays.<Integer>asList(0);
        }
        int m = X.length;
        int n = X0.length;
        List<Integer> Y0 = new ArrayList<Integer>();
        double[] cp_Y = new double[m];
        for (int i1 = 0; i1 < n; i1++) {//遍历X0
            int j = 0;
            copy_vector(Y, cp_Y);
            int kk = j;
            /*求各级均差*/
            while (kk < m - 1) {
                kk = kk + 1;
                for (int i2 = kk; i2 < m; i2++) {
                    cp_Y[i2] = (cp_Y[i2] - cp_Y[kk - 1]) / (X[i2] - X[kk - 1]);
                }
            }
            /*求插值结果*/
            double temp = cp_Y[0];
            for (int i = 1; i <= m - 1; i++) {
                double u = 1;
                int jj = 0;
                while (jj < i) {
                    u *= (X0[i1] - X[jj]);
                    jj++;
                }
                temp += cp_Y[i] * u;
            }

            Y0.add((int) temp);
        }

        Integer yTotal = 0;
        for (Integer v : Y0) {
            yTotal += v;
        }
        if (total > yTotal) {
            Y0.set(0, Y0.get(0) + (total - yTotal));
        } else if (total < yTotal) {
            int diff = yTotal - total;
            for (int i = Y0.size() - 1; i >= 0; i--) {
                int yValue = Y0.get(i);
                if (yValue > diff) {
                    Y0.set(i, yValue - diff);
                    break;
                } else {
                    diff = diff - yValue;
                    Y0.remove(i);
                }
            }
        }

        //去掉零值
        for (int i = Y0.size() - 1; i >= 0; i--) {
            if (Y0.get(i) <= 0) {
                Y0.remove(i);
            }
        }

        return Y0;
    }

    public static void main(String[] args) {
        /*输入插值点横纵坐标*/
        System.out.println("Input number of interpolation point:");

        double X[] = new double[2];
        double Y[] = new double[2];
        double X0[] = new double[5];

        X[0] = 20160601;
        Y[0] = 0;
        X[1] = 20160630;
        Y[1] = 1;

        for (int i = 0; i < 5; i++) {
            X0[i] = X[0] + i + 1;
        }

        List<Integer> doubles = Newton_inter_method(X, Y, X0, 1);
        for (double aDouble : doubles) {
            System.out.println(aDouble);
        }

    }

}

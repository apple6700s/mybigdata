package com.dt.mig.sync.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * com.dt.mig.sync.utils.QuestionAnswerUtil
 *
 * @author zhaozhen
 * @since 2017/7/7
 */
public class QuestionAnswerUtil {

    public static Integer[] getTrendValue(String value) {
        Integer[] trendLike = new Integer[2];
        String[] split = value.split("\\|");

        if (split != null && split.length >= 2) {
            //trendLike[0] 是点赞数，[1]是评论数
            if (isNumeric(split[1]) && !split[1].equals("")) {
                System.out.println(split[1]);
                trendLike[1] = Integer.parseInt(split[1]);
                System.out.println(split[1]);
            } else {
                System.out.println("ERROR TREND VALUE:" + split[1]);
            }
        } else {
            System.out.println("ERROR TREND VALUE:" + value);
        }
        if (split != null && split.length >= 3) {
            if (isNumeric(split[2]) && !split[2].equals("")) {
                trendLike[0] = Integer.parseInt(split[2]);
            } else {
                System.out.println("ERROR TREND VALUE:" + split[2]);
            }

        } else {
            System.out.println("ERROR TREND VALUE:" + value);
        }
        return trendLike;
    }

    public static boolean isNumeric(String str) {
        Pattern pattern = Pattern.compile("[0-9]*");
        Matcher isNum = pattern.matcher(str);
        if (!isNum.matches()) {
            return false;
        }
        return true;
    }

    public static void main(String[] args) {
        getTrendValue("||5|");
    }
}

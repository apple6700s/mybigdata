package com.datastory.banyan.weibo.analyz;


/**
 * com.datastory.banyan.weibo.analyz.BirthYearExtractor
 *
 * @author lhfcws
 * @since 16/11/23
 */

public class BirthYearExtractor {
    public static String extract(String birthdate) {
        if (birthdate == null || birthdate.length() < 4)
            return null;
        try {
            String birthYear = birthdate.substring(0, 4);
            Integer y = Integer.parseInt(birthYear);
            if (y > 1901 && y < 2050)
                return birthYear;
        } catch (Exception ignore) {
        }
        return null;
    }
}

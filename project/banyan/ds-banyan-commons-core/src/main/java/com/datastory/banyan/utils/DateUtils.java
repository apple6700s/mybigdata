package com.datastory.banyan.utils;

import com.yeezhao.commons.util.Pair;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * com.datatub.rhino.utils.DateUtils
 *
 * @author lhfcws
 * @since 2016/11/7
 */
public class DateUtils {
    public static final String PRETTY_TIMEFORMAT = "yyyy-MM-dd HH:mm:ss";
    public static final String DFT_TIMEFORMAT = "yyyyMMddHHmmss";
    public static final String DFT_DAYFORMAT = "yyyyMMdd";
    public static final String DFT_HOURFORMAT = "yyyyMMddHH";

    public static String getCurrentPrettyTimeStr() {
        SimpleDateFormat sdf = new SimpleDateFormat(PRETTY_TIMEFORMAT);
        return sdf.format(new Date());
    }

    public static String getCurrentDateStr() {
        SimpleDateFormat sdf = new SimpleDateFormat(DFT_DAYFORMAT);
        return sdf.format(new Date());
    }

    public static String getCurrentTimeStr() {
        SimpleDateFormat sdf = new SimpleDateFormat(DFT_TIMEFORMAT);
        return sdf.format(new Date());
    }

    public static String getCurrentHourStr() {
        SimpleDateFormat sdf = new SimpleDateFormat(DFT_HOURFORMAT);
        return sdf.format(new Date());
    }

    public static String getHourStr(Date date) {
        if (date == null)
            return null;
        SimpleDateFormat sdf = new SimpleDateFormat(DFT_HOURFORMAT);
        return sdf.format(date);
    }

    public static String getDateStr(Date date) {
        if (date == null)
            return null;
        SimpleDateFormat sdf = new SimpleDateFormat(DFT_DAYFORMAT);
        return sdf.format(date);
    }

    public static String getTimeStr(Date date) {
        if (date == null)
            return null;
        SimpleDateFormat sdf = new SimpleDateFormat(DFT_TIMEFORMAT);
        return sdf.format(date);
    }

    public static boolean validateDatetime(String date) {
        if (date == null)
            return false;
        return ((date.startsWith("201") || date.startsWith("200")) && date.length() == DFT_TIMEFORMAT.length());
    }

    public static boolean validateDate(String date) {
        if (date == null)
            return false;
        return ((date.startsWith("201") || date.startsWith("200")) && date.length() == DFT_DAYFORMAT.length());
    }

    public static Date parse(String dateStr, String formatStr) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat(formatStr);
        return sdf.parse(dateStr);
    }

    public static Pair<String, String> genLastHourRange(Date d) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(d);

        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        String endDate = getTimeStr(calendar.getTime());

        calendar.add(Calendar.HOUR_OF_DAY, -1);
        String startDate = getTimeStr(calendar.getTime());
        return new Pair<>(startDate, endDate);
    }

    public static Pair<String, String> genDateRange(Date d) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(d);

        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        String startDate = getTimeStr(calendar.getTime());

        calendar.add(Calendar.DATE, 1);
        String endDate = getTimeStr(calendar.getTime());

        return new Pair<>(startDate, endDate);
    }

    public static void main(String[] args) {
        System.out.println("[PROGRAM] Program started.");
        System.out.println(validateDate("{publish_date}"));
        System.out.println("[PROGRAM] Program exited.");
    }
}

package com.dt.mig.sync.utils;

import com.yeezhao.commons.util.Pair;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * com.datatub.rhino.utils.DateUtils
 *
 * @author lhfcws test
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
        if (date == null) return null;
        SimpleDateFormat sdf = new SimpleDateFormat(DFT_HOURFORMAT);
        return sdf.format(date);
    }

    public static String getDateStr(Date date) {
        if (date == null) return null;
        SimpleDateFormat sdf = new SimpleDateFormat(DFT_DAYFORMAT);
        return sdf.format(date);
    }

    public static String getTimeStr(Date date) {
        if (date == null) return null;
        SimpleDateFormat sdf = new SimpleDateFormat(DFT_TIMEFORMAT);
        return sdf.format(date);
    }

    public static Long timeStr2timestamp(String s) {
        if (s == null) return null;
        try {
            Date d = parse(s, DFT_TIMEFORMAT);
            return d.getTime();
        } catch (ParseException e) {
            return null;
        }
    }

    public static boolean validateDatetime(String date) {
        if (date == null) return false;
        return ((date.startsWith("201") || date.startsWith("200")) && date.length() == DFT_TIMEFORMAT.length());
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

    /**
     * 计算两个日期之间相差的天数
     *
     * @param smdate 较小的时间
     * @param bdate  较大的时间
     * @return 相差天数
     * @throws java.text.ParseException
     */
    public static int daysBetween(Date smdate, Date bdate) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        smdate = sdf.parse(sdf.format(smdate));
        bdate = sdf.parse(sdf.format(bdate));
        Calendar cal = Calendar.getInstance();
        cal.setTime(smdate);
        long time1 = cal.getTimeInMillis();
        cal.setTime(bdate);
        long time2 = cal.getTimeInMillis();
        long between_days = (time2 - time1) / (1000 * 3600 * 24);

        return Integer.parseInt(String.valueOf(between_days));
    }

    public static void main(String[] args) {

        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
            Date date1 = sdf.parse("2016-06-01 10:00:00");
            Date date2 = sdf.parse("2016-06-01 00:00:00");
            System.out.println(daysBetween(date1, date2));
            System.out.println(daysBetween(date2, date1));
        } catch (ParseException e) {
            e.printStackTrace();
        }

        System.out.println("[PROGRAM] Program started.");
        System.out.println(getCurrentHourStr());
        System.out.println("[PROGRAM] Program exited.");
    }
}

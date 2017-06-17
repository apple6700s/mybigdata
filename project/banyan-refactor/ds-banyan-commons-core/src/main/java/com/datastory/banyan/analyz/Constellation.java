package com.datastory.banyan.analyz;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * com.datatub.rhino.analyz.Constellation
 *
 * @author lhfcws
 * @since 2016/11/3
 */
public enum Constellation {
    ARIES("白羊"), // 白羊
    TAURUS("金牛"), // 金牛
    GEMINI("双子"), // 双子
    CANCER("巨蟹"), // 巨蟹
    LEO("狮子"), // 狮子
    VIRGO("处女"), // 处女
    LIBRA("天秤"), // 天秤
    SCORPIO("天蝎"), // 天蝎
    SAGITTARIUS("射手"), // 射手
    CAPRICORN("摩羯"), // 摩羯
    AQUARIUS("水瓶"), // 水瓶
    PISCES("双鱼"); // 双鱼

    private String name;

    Constellation(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public static Constellation identifyConstellation(String birthDate) {
        Pattern pattern = Pattern.compile("\\d{4}-\\d{1,2}-\\d{1,2}");
        Matcher matcher = pattern.matcher(birthDate);

        Constellation constellation = null;
        if (matcher.find()) {
            String[] segs = birthDate.split("-");
            if (Integer.parseInt(segs[1]) == 0 || Integer.parseInt(segs[2]) == 0)
                return null;
            int birthTime = Integer.parseInt(segs[1]) * 100 + Integer.parseInt(segs[2]);
            if (birthTime >= 120 && birthTime <= 218) {
                constellation = AQUARIUS;
            }
            else if (birthTime >= 219 && birthTime <= 320) {
                constellation = PISCES;
            }
            else if (birthTime >= 321 && birthTime <= 419) {
                constellation = ARIES;
            }
            else if (birthTime >= 420 && birthTime <= 520) {
                constellation = TAURUS;
            }
            else if (birthTime >= 521 && birthTime <= 621) {
                constellation = GEMINI;
            }
            else if (birthTime >= 622 && birthTime <= 722) {
                constellation = CANCER;
            }
            else if (birthTime >= 723 && birthTime <= 822) {
                constellation = LEO;
            }
            else if (birthTime >= 823 && birthTime <= 922) {
                constellation = VIRGO;
            }
            else if (birthTime >= 923 && birthTime <= 1023) {
                constellation = LIBRA;
            }
            else if (birthTime >= 1024 && birthTime <= 1122) {
                constellation = SCORPIO;
            }
            else if (birthTime >= 1123 && birthTime <= 1221) {
                constellation = SAGITTARIUS;
            }
            else {
                constellation = CAPRICORN;
            }
        }
        else {
            if (birthDate.contains("白羊")) {
                constellation = ARIES;
            }
            else if (birthDate.contains("金牛")) {
                constellation = TAURUS;
            }
            else if (birthDate.contains("双子")) {
                constellation = GEMINI;
            }
            else if (birthDate.contains("巨蟹")) {
                constellation = CANCER;
            }
            else if (birthDate.contains("狮子")) {
                constellation = LEO;
            }
            else if (birthDate.contains("处女")) {
                constellation = VIRGO;
            }
            else if (birthDate.contains("天秤")) {
                constellation = LIBRA;
            }
            else if (birthDate.contains("天蝎")) {
                constellation = SCORPIO;
            }
            else if (birthDate.contains("射手")) {
                constellation = SAGITTARIUS;
            }
            else if (birthDate.contains("摩羯")) {
                constellation = CAPRICORN;
            }
            else if (birthDate.contains("水瓶")) {
                constellation = AQUARIUS;
            }
            else if (birthDate.contains("双鱼")) {
                constellation = PISCES;
            }
        }
        return constellation;
    }

    public static void main(String[] args) {
        System.out.println("[PROGRAM] Program started.");
        System.out.println(Constellation.identifyConstellation("1996-04-09").getName());
        System.out.println("[PROGRAM] Program exited.");
    }
}
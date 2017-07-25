package com.datastory.banyan.weibo.analyz;

import com.yeezhao.commons.util.Entity.StrParams;
import com.yeezhao.commons.util.StringUtil;
import com.yeezhao.commons.util.Pair;
import com.yeezhao.commons.util.ILineParser;
import com.yeezhao.commons.util.AdvFile;
import com.yeezhao.commons.util.ClassUtil;
import com.yeezhao.commons.util.serialize.GsonSerializer;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

/**
 * 地址转换器
 * <p>
 * com.datatub.rhino.analyz.AreaTranslator
 *
 * @author lhfcws
 * @since 2016/10/17
 */
public class AreaTranslator {
    private static final Set<String> MUNICIPALITY_NAME = new HashSet<String>(){{
        add("北京");
        add("上海");
        add("天津");
        add("重庆");
    }};

    private static final Set<String> NOT_MAIN_LAND = new HashSet<String>() {{
        add("海外");
        add("香港");
        add("澳门");
        add("台湾");
    }};
    private static final StrParams areaMap = new StrParams();
    private static final StrParams cityLevelMap = new StrParams();

    static {
        init();
    }

    public static List<String> getKeys() {
        return new ArrayList<>(areaMap.keySet());
    }

    public static String getProvince(String provId) {
        if (StringUtil.isNullOrEmpty(provId))
            return "";
        String v = areaMap.get(provId);
        return v == null ? "" : v;
    }

    public static String getCity(String provId, String cityId) {
        if (StringUtil.isNullOrEmpty(provId) || StringUtil.isNullOrEmpty(cityId))
            return "";
        String v = areaMap.get(provId + "-" + cityId);
        return v == null ? "" : v;
    }

    public static Pair<String, String> getProvinceNCity(String provId, String cityId) {
        if (StringUtil.isNullOrEmpty(provId) || StringUtil.isNullOrEmpty(cityId))
            return null;
        String city = getCity(provId, cityId);
        String prov = getProvince(provId);
        return new Pair<>(prov, city);
    }

    public static String getCityLevel(String city) {
        if (StringUtil.isNullOrEmpty(city))
            return null;
        String v = cityLevelMap.get(city);
        return v;
    }

    public static boolean isMunicipality(String province) {
        return MUNICIPALITY_NAME.contains(province);
    }

    public static boolean isNotMainLand(String area) {
        return NOT_MAIN_LAND.contains(area);
    }

    /**
     * static init
     */
    private static void init() {
        // init area json by resource file area.json
        try {
            String areaJson = ClassUtil.getResourceAsString("area.json");
            if (areaJson != null) {
                AreaJson obj = GsonSerializer.deserialize(areaJson, AreaJson.class);
                List<AreaJsonProvinceEntry> plist = obj.get("provinces");
                for (AreaJsonProvinceEntry pe : plist) {
                    areaMap.put(pe.getId(), pe.getName());
                    for (StrParams p : pe.getCitys()) {
                        for (Map.Entry<String, String> e : p.entrySet()) {
                            areaMap.put(pe.getId() + "-" + e.getKey(), e.getValue());
                        }
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        // init city_level
        try {
            InputStream in = ClassUtil.getResourceAsInputStream("city_level.txt");
            if (in != null)
                AdvFile.loadFileInLines(in, new ILineParser() {
                    @Override
                    public void parseLine(String s) {
                        String[] items = s.split(":");
                        String levelId = items[0];
                        if (items.length > 1) {
                            String[] cities = items[1].split(",");
                            for (String city : cities)
                                cityLevelMap.put(city, levelId);
                        }
                    }
                });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * inner classes for json serialization
     */
    public static class AreaJson extends HashMap<String, List<AreaJsonProvinceEntry>> {
    }

    public static class AreaJsonProvinceEntry {
        String id;
        String name;
        List<StrParams> citys;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public List<StrParams> getCitys() {
            return citys;
        }

        public void setCitys(List<StrParams> citys) {
            this.citys = citys;
        }
    }

    public static void main(String[] args) {
        System.out.println(AreaTranslator.getCityLevel("广州"));
        System.out.println(AreaTranslator.getCity("46", "90"));
    }
}

package com.datastory.banyan.weibo.analyz;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.yeezhao.commons.util.Pair;
import org.apache.commons.lang3.StringUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


/**
 * @author sezina
 * @since 10/9/16
 */
public class MigrationUtil implements Serializable {

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

    private final Map<String, String> PROVINCE_CODE_MAP = new HashMap<>();
    private final Map<String, String> CITY_CODE_MAP = new HashMap<>();

    public MigrationUtil() {
        init();
    }

    public void init() {
        String json = "";
        String line;
        BufferedReader br = new BufferedReader(new InputStreamReader(this.getClass().getClassLoader().getResourceAsStream("area.json")));
        try {
            while ((line = br.readLine()) != null) {
                json += line.trim();
            }
            JSONObject jsonObject = JSONObject.parseObject(json);
            JSONArray provinces = jsonObject.getJSONArray("provinces");
            for (int i = 0; i < provinces.size(); i++) {
                JSONObject pj = provinces.getJSONObject(i);
                String pCode = pj.getString("id");
                String pName = pj.getString("name");
                PROVINCE_CODE_MAP.put(pName, pCode);
                JSONArray cities = pj.getJSONArray("citys");
                for (int j = 0; j < cities.size(); j++) {
                    JSONObject cj = cities.getJSONObject(j);
                    Set<String> keys = cj.keySet();
                    for (String key : keys) {
                        CITY_CODE_MAP.put(cj.getString(key), key);
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Pair<String ,String> getProvinceAndCity(String locationFormat) {
        if (StringUtils.isNotEmpty(locationFormat)) {
            String area = locationFormat;
            String province = area;
            String city = null;
            String[] areas = area.split("#");
            if (areas.length>=2) {
                province=areas[1];
                if(areas.length >= 3){
                    city = areas[2]; //形如:四川#成都
                }
            }
            return new Pair<>(province, city);
        }
        return null;
    }

    // TODO: implementation
    public String getProvinceCode(String province) {
        if (province != null) {
            return PROVINCE_CODE_MAP.get(province);
        }
        return null;
    }

    public String getCityCode(String city) {
        if (city != null) {
            return CITY_CODE_MAP.get(city);
        }
        return null;
    }

    public boolean isMunicipality(String province) {
        return MUNICIPALITY_NAME.contains(province);
    }

    public boolean isNotMainLand(String area) {
        return NOT_MAIN_LAND.contains(area);
    }
}

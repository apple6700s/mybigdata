package com.datastory.banyan.weibo.analyz;

import com.yeezhao.commons.util.StringUtil;
import org.apache.commons.lang3.StringUtils;
import weibo4j.model.*;
import weibo4j.org.json.*;

import java.io.Serializable;
import java.util.*;

/**
 * com.datatub.rhino.wbuser.analyz.AdvUserJsonProcessor
 *
 * @author lhfcws
 * @since 2016/11/10
 */
public class AdvUserJsonProcessor implements Serializable {

    /**
     * common process
     */
    public static Object process(String type, String json) throws Exception {
        try {
            JSONObject jsonObject = new JSONObject(json);
            if ("bilateral_follow".equals(type))
                return processFollowers(jsonObject);
            else if ("birthday".equals(type))
                return processBirthday(jsonObject);
            if ("career".equals(type))
                return processCareer(jsonObject);
            else if ("education".equals(type))
                return processEducation(jsonObject);
            else if ("tags".equals(type))
                return processTags(jsonObject);
        } catch (JSONException e) {
            JSONArray jsonArray = new JSONArray(json);
            if ("career".equals(type))
                return processCareer(jsonArray);
            else if ("education".equals(type))
                return processEducation(jsonArray);
            else if ("tags".equals(type))
                return processTags(jsonArray);
        }
        throw new WeiboException("Not a validate type");
    }

    public static String processCareer(JSONObject jsonObject) throws JSONException {
        JSONArray jsonArray = jsonObject.getJSONArray("json");
        return processCareer(jsonArray);
    }

    public static String processCareer(JSONArray jsonArray) throws JSONException {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < jsonArray.length(); i++) {
            JSONObject object = jsonArray.getJSONObject(i);
            try {
                Career career = new Career(object);
                if (sb.length() > 0) {
                    sb.append(StringUtil.DELIMIT_1ST);
                }
                sb.append(career.getCompany() + StringUtil.DELIMIT_2ND
                        + career.getDepartment() + StringUtil.DELIMIT_2ND
                        + career.getStart() + StringUtil.DELIMIT_2ND
                        + career.getEnd() + StringUtil.DELIMIT_2ND
                        + career.getProvince() + StringUtil.DELIMIT_2ND
                        + career.getCity());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return sb.toString();
    }

    public static String processBirthday(JSONObject jsonObject) throws JSONException, WeiboException {
        JSONObject object = jsonObject.getJSONObject("json");
        if (object == null)
            object = jsonObject;

        Profile profile = new Profile(object);

        return profile.getBirthday();
    }

    public static String processEducation(JSONObject jsonObject) throws JSONException {
        JSONArray jsonArray = jsonObject.getJSONArray("json");
        return processEducation(jsonArray);
    }

    public static String processEducation(JSONArray jsonArray) throws JSONException {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < jsonArray.length(); i++) {
            JSONObject object = jsonArray.getJSONObject(i);
            try {
                Education e = new Education(object);
                if (sb.length() > 0) {
                    sb.append(StringUtil.DELIMIT_1ST);
                }
                sb.append(e.getSchool() + StringUtil.DELIMIT_2ND
                        + e.getDepartment() + StringUtil.DELIMIT_2ND
                        + e.getYear());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return sb.toString();
    }

    public static HashMap<String, String> processFollowers(JSONObject jsonObject) throws JSONException {
        HashMap<String, String> map = new HashMap<>();

        JSONObject object = jsonObject.getJSONObject("json");
        if (object == null) {
            object = jsonObject;
        }
        if (object.has("follow")) {
            JSONArray array = object.getJSONArray("follow");
            if (array != null) {
                for (int i = 0; i < array.length(); i++) {
                    map.put(array.getString(i), "");
                }
            }
        }

        if (object.has("bilateral")) {
            JSONArray array = object.getJSONArray("bilateral");
            if (array != null) {
                for (int i = 0; i < array.length(); i++) {
                    map.put(array.getString(i), "1");
                }
            }
        }

        return map;
    }

    public static String processTags(JSONObject jsonObject) throws JSONException {
        JSONArray jsonArray = jsonObject.getJSONArray("json");
        return processTags(jsonArray);
    }

    public static String processTags(JSONArray jsonArray) throws JSONException {
        List<String> tags = new ArrayList<String>();
        for (int i = 0; i < jsonArray.length(); i++) {
            JSONObject object = jsonArray.getJSONObject(i);
            try {
                Tag tag = new Tag(object);
                tags.add(tag.getValue());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return StringUtils.join(tags, StringUtil.DELIMIT_1ST);
    }
}

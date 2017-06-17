package com.datastory.banyan.weibo.util;

/**
 * Created by abel.chan on 16/12/22.
 */
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class WeiboUtils {
    public static final String IGNORE_TOKEN_FLAG = "_IGNORE_";
    private static String[] str62keys = new String[]{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z", "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z"};
    private static JSONObject _locationJSON = null;

    private static JSONObject locationJSON() {
        if(_locationJSON == null) {
            Runtime runtime = Runtime.getRuntime();
            InputStreamReader reader = null;
            BufferedReader br = null;

            try {
                Process e = runtime.exec("curl http://api.t.sina.com.cn/provinces.json");
                reader = new InputStreamReader(e.getInputStream());
                br = new BufferedReader(reader);
                StringBuilder sb = new StringBuilder();
                String line = null;

                while((line = br.readLine()) != null) {
                    line.replace("\n", "");
                    sb.append(line);
                }

                _locationJSON = new JSONObject(sb.toString());
            } catch (Exception var14) {
                System.out.println(var14.getMessage());
                var14.printStackTrace();
            } finally {
                try {
                    br.close();
                    reader.close();
                } catch (IOException var13) {
                    var13.printStackTrace();
                }

            }
        }

        return _locationJSON;
    }

    public static String getLocation(int province, int city) {
        if(province == -1) {
            return "NULL-NULL";
        } else {
            String result = "";

            try {
                JSONArray e = locationJSON().getJSONArray("provinces");

                for(int i = 0; i < e.length(); ++i) {
                    JSONObject prov = (JSONObject)e.get(i);
                    if(province == prov.getInt("id")) {
                        result = prov.getString("name") + "-";
                        if(city == -1) {
                            return result + "NULL";
                        }

                        String cs = Integer.toString(city);
                        JSONArray cities = prov.getJSONArray("citys");
                        int j = 0;

                        while(j < cities.length()) {
                            JSONObject c = (JSONObject)cities.get(j);

                            try {
                                String e1 = c.getString(cs);
                                if(e1 == null) {
                                    throw new Exception();
                                }

                                return result + e1;
                            } catch (Exception var11) {
                                ++j;
                            }
                        }

                        return result + "NULL";
                    }
                }
            } catch (JSONException var12) {
                var12.printStackTrace();
            }

            return "NULL-NULL";
        }
    }

    public static String url2mid(String url) {
        String mid = "";

        for(int index = url.length(); index > 0; index -= 4) {
            String substr = "";
            if(index - 4 < 0) {
                substr = url.substring(0, index);
                mid = int62to10(substr, false) + mid;
            } else {
                substr = url.substring(index - 4, index);
                mid = int62to10(substr, true) + mid;
            }
        }

        return mid;
    }

    public static String mid2url(String mid) {
        String url = "";
        boolean needPending = true;

        for(int i = mid.length() - 7; i > -7; i -= 7) {
            int offset1 = i < 0?0:i;
            int offset2 = i + 7;
            String num = mid.substring(offset1, offset2);
            if(i < 0) {
                needPending = false;
            }

            num = int10to62(Integer.parseInt(num), needPending);
            url = num + url;
        }

        return url;
    }

    public static String int62to10(String int62, boolean needPending) {
        int res = 0;
        byte base = 62;

        for(int resstr = 0; resstr < int62.length(); ++resstr) {
            res *= base;
            char charofint62 = int62.charAt(resstr);
            int num;
            if(charofint62 >= 48 && charofint62 <= 57) {
                num = charofint62 - 48;
                res += num;
            } else if(charofint62 >= 97 && charofint62 <= 122) {
                num = charofint62 - 97 + 10;
                res += num;
            } else {
                if(charofint62 < 65 || charofint62 > 90) {
                    System.err.println("this is not a 62base number");
                    return null;
                }

                num = charofint62 - 65 + 36;
                res += num;
            }
        }

        String var7 = String.valueOf(res);
        if(needPending) {
            while(var7.length() < 7) {
                var7 = "0" + var7;
            }
        }

        return var7;
    }

    public static String int10to62(int int10, boolean needPending) {
        String s62 = "";

        for(boolean r = false; int10 != 0 && s62.length() < 100; int10 = (int)Math.floor((double)(int10 / 62))) {
            int r1 = int10 % 62;
            s62 = str62keys[r1] + s62;
        }

        if(needPending) {
            while(s62.length() < 4) {
                s62 = "0" + s62;
            }
        }

        return s62;
    }

    public static String getWeiboUrl(String uid, String mid) {
        try {
            String link = String.format("http://weibo.com/%s/%s"
                    , new Object[]{uid, mid2url(mid)});
            return link;
        } catch (Exception e) {
            return null;
        }
    }
}


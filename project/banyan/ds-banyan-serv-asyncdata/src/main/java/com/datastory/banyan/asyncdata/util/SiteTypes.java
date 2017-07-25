package com.datastory.banyan.asyncdata.util;

import com.yeezhao.commons.util.AdvFile;
import com.yeezhao.commons.util.ClassUtil;
import com.yeezhao.commons.util.ILineParser;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

/**
 * com.datastory.banyan.asyncdata.util.SiteTypes
 *
 * @author lhfcws
 * @since 2017/4/10
 */
public class SiteTypes {

    public static final String T_ECOM = "电商";
    public static final String T_VIDEO = "视频";

    private static volatile SiteTypes _singleton = null;

    public static SiteTypes getInstance() throws IOException {
        if (_singleton == null) {
            synchronized (SiteTypes.class) {
                if (_singleton == null) {
                    _singleton = new SiteTypes();
                }
            }
        }
        return _singleton;
    }

    private HashMap<String, String> name2type = new HashMap<>();
    private HashMap<String, Set<String>> type2names = new HashMap<>();

    private SiteTypes() throws IOException {
        load();
    }

    private void load() throws IOException {
        InputStream in = ClassUtil.getResourceAsInputStream("video-ecom.mapping.csv");

        AdvFile.loadFileInDelimitLine(in, new ILineParser() {
            @Override
            public void parseLine(String s) {
                s = s.trim();
                if (s.startsWith("#"))
                    return;

                String[] arr = s.split(",");
                name2type.put(arr[0], arr[2]);
                if (!type2names.containsKey(arr[2]))
                    type2names.put(arr[2], new HashSet<String>());
                type2names.get(arr[2]).add(arr[0]);
            }
        });
    }

    public String getType(String name) {
        return name2type.get(name);
    }
}

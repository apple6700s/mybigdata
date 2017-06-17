package com.datastory.banyan.utils;

import com.datastory.banyan.hbase.PhoenixWriter;
import com.yeezhao.commons.util.Entity.Params;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.*;

/**
 * com.datastory.banyan.utils.HBaseCSV
 *
 * @author lhfcws
 * @since 2017/1/10
 */
public class HBaseCSV implements Serializable, Iterator<Params> {
    static Logger LOG = Logger.getLogger(HBaseCSV.class);

    String path;
    String table;
    String[] headers;
    BufferedReader br;
    FileOutputStream out;
    String nextLine;
    String sep = "\t";

    public HBaseCSV(String table, String path) {
        this.path = path;
        this.table = table;
    }

    public String[] getHeaders() {
        return headers;
    }

    public static HBaseCSV load(String table, String path) throws IOException {
        HBaseCSV csv = new HBaseCSV(table, path);

        FileInputStream fis = new FileInputStream(path);
        InputStreamReader reader = new InputStreamReader(fis);
        csv.br = new BufferedReader(reader);

        String _header = csv.br.readLine();
        csv.headers = _header.trim().split("\t");
        for (int i = 0; i < csv.headers.length; i++) {
            if (csv.headers[i].startsWith("'"))
                csv.headers[i] = csv.headers[i].substring(1, csv.headers[i].length() - 1);
        }

        System.out.println(Arrays.toString(csv.headers));

        return csv;
    }

    public void beginSave() throws IOException {
        PhoenixWriter writer = new PhoenixWriter() {
            @Override
            public String getTable() {
                return table;
            }
        };

        headers = new String[writer.getFields().size()];
        headers = writer.getFields().toArray(headers);

        out = new FileOutputStream(path);
        String line =StringUtils.join(headers, sep) + "\n";
        out.write(line.getBytes("utf-8"));
    }

    public void save(Map<String, ? extends Object> p) throws IOException {
        List<String> list = new ArrayList<>();
        for (String h : headers) {
            Object v = p.get(h);
            if (v instanceof String) {
                String nv = ((String) v).replaceAll("\t", " ");
                list.add(nv);
            } else
                list.add(String.valueOf(v));
        }
        String line = StringUtils.join(list, sep) + "\n";
        out.write(line.getBytes("utf-8"));
    }

    public void endSave() {
        if (out != null) {
            try {
                out.flush();
                out.close();
                out = null;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public boolean hasNext() {
        try {
            nextLine = br.readLine();
            return nextLine != null;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public Params next() {
//        char[] chars = nextLine.toCharArray();
//        for (int i = 1; i < chars.length - 1; i++) {
//            if (chars[i] == ',') {
//                boolean b = ((chars[i - 1] == '\'' || chars[i - 1] == 'l') &&
//                        (chars[i + 1] == '\'' || chars[i + 1] == 'n')
//                        );
//                if (!b) {
//                    chars[i] = '，';
//                }
//            }
//        }
//        String line = String.valueOf(chars);
        String line = nextLine;
        String[] arr = line.trim().split("\t");
        Params p = new Params();
        try {
            for (int i = 0; i < headers.length; i++) {
                String s = arr[i];
                if ("null".equals(s))
                    s = null;
                p.put(headers[i], s);
            }
        } catch (Exception ignore) {
            LOG.error(ignore.getMessage(), ignore);
            return null;
        }
        return p;
    }

    public void close() {
        if (br != null) {
            try {
                br.close();
                br = null;
            } catch (IOException e) {

            }
        }
        endSave();
    }

    @Override
    public void remove() {

    }

}

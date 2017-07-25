package com.datastory.banyan.asyncdata.ecom.analyz;

import com.datastory.banyan.utils.ErrorUtil;
import com.yeezhao.commons.util.ClassUtil;
import com.yeezhao.commons.util.Pair;

import java.io.IOException;
import java.util.*;


/**
 * com.datastory.banyan.asyncdata.ecom.analyz.PriceExtractor
 *
 * @author lhfcws
 * @since 2017/4/12
 */
public class PriceExtractor {
    private static final String DFT_Y = "￥";    // dft for CNY
    private static final String DFT_S = "$";    // dft for USD

    private static Map<String, Double> currencyRates = new HashMap<>();

    static {
        try {
            String lines = ClassUtil.getResourceAsString("currency_rate.txt");
            for (String line : lines.split("\n")) {
                String[] items = line.split(" ");
                Double rate = Double.valueOf(items[2]);
                currencyRates.put(items[0] + "-" + items[1], rate);
                currencyRates.put(items[1] + "-" + items[0], 1.0 / rate);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Pair<String, String> extract(String price) {
        if (price.contains(DFT_S)) {
            price = price.replace(DFT_S, "").replaceAll(" ", "");
            return new Pair<>("USD", price);
        } else {
            price = price.replace(DFT_Y, "").replaceAll(" ", "");
            return new Pair<>("CNY", price);
        }
    }

    /**
     * 全部换算成人民币，汇率换算后默认为整数元。
     *
     * @param price
     * @return
     */
    public String extractCNY(String price) {
        if (price == null) return price;

        price = price.replaceAll("元|块|圆|,| ", "");
        if (price.contains(DFT_S)) {
            price = price.replace(DFT_S, "");
            try {
                Double d = Double.valueOf(price);
                Double rate = currencyRates.get("USD-CNY");
                return "" + new Double(d * rate).intValue();
            } catch (Exception e) {
                ErrorUtil.simpleError(price, e);
                return price;
            }
        } else {
            price = price.replace(DFT_Y, "");
            return price;
        }
    }
}

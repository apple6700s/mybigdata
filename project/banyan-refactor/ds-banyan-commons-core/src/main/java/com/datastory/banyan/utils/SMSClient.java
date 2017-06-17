package com.datastory.banyan.utils;

import com.datastory.banyan.base.RhinoETLConfig;
import com.datatub.ops.pigeon.client.BasicInfoEntity;
import com.datatub.ops.pigeon.client.PigeonClient;
import com.datatub.ops.pigeon.client.SMSRequestEntity;
import com.yeezhao.commons.util.StringUtil;
import com.yeezhao.commons.util.serialize.FastJsonSerializer;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;

/**
 * com.datastory.banyan.utils.SMSClient
 * <p>
 * sms sender client
 *
 * @author lhfcws
 * @since 2017/2/8
 */
public class SMSClient {
    // 短信要钱，所以测试调试时加开关。
    public static volatile boolean DEBUG = false;
    public static final HashSet<String> PHONES;

    static {
        Configuration conf = RhinoETLConfig.getInstance();
        String[] arr = conf.get("alarm.sms.receiver").split(";|,");
        ArrayList<String> list = new ArrayList<>();
        for (String phone : arr) {
            if (!StringUtil.isNullOrEmpty(phone)) {
                list.add(phone);
            }
        }
        PHONES = new HashSet<>(list);
    }

    public static final String GROUP = "ds";
    public static final String PROJECT = "ds-banyan";

    public static final String ES_ERROR_ALARM = "es-error-alarm";

    public static final String[] TITLES = {
            ES_ERROR_ALARM, // #Banyan全量库前一天ES数据异常告警# 故障日期: {1} 。es库: {2} 。故障: {3}
    };

    public static void send(String templateKey, String... argss) {
        BasicInfoEntity basicEntity = new BasicInfoEntity();
        basicEntity.setProject(PROJECT);
        basicEntity.setTeam(GROUP);
        basicEntity.setTopic(templateKey);
        SMSRequestEntity request = new SMSRequestEntity(basicEntity);
        request.setReceivers(PHONES);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < argss.length; i++) {
            argss[i] = argss[i].replaceAll(",", "，");
            sb.append(argss[i]).append(",");
        }
        if (argss.length > 0) {
            sb.setLength(sb.length() - 1);
            request.setParams(sb.toString());
        }

        System.out.println("======== " + new Date());
        System.out.println(FastJsonSerializer.serialize(request));
        System.out.println("===========================");
        if (!DEBUG)
            PigeonClient.sendSMS(request);
    }

    /**
     * Inovke main
     */
    public static void main(String[] args) throws Exception {
        System.out.println("[PROGRAM] Program started. PID=" + ManagementFactory.getRuntimeMXBean().getName().split("@")[0]);
        SMSClient.send(args[0], ArrayUtils.subarray(args, 1, args.length));
        System.out.println("[PROGRAM] Program exited.");
    }
}

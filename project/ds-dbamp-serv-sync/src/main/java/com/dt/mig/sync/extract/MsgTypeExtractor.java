package com.dt.mig.sync.extract;

import com.yeezhao.commons.util.StringUtil;

import java.lang.management.ManagementFactory;

/**
 * Created by abel.chan on 16/12/23.
 */
public class MsgTypeExtractor {

    public static boolean valid(String s) {
        return !StringUtil.isNullOrEmpty(s) && !"null".equals(s);
    }

    /**
     * @param srcMid   博文最原始的id
     * @param rtMid    pid
     * @param selfText 自身发布的内容
     * @param text     全部内容,包括自身发布的内容
     * @return -1 未知,0 原贴, 1 一层转发不带文字 2 一层转发待文字
     */
    public static short analyz(String srcMid, String rtMid, String selfText, String text) {
        if (!valid(text)) return -1;
        if (!valid(srcMid)) return 0;

        if ((!valid(rtMid) && !text.contains("//@")) || (valid(rtMid) && rtMid.equals(srcMid))) {
            if (valid(selfText)) {
                return 2;
            } else {
                return 1;
            }

        } else {
            return 3;
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[PROGRAM] Program started. PID=" + ManagementFactory.getRuntimeMXBean().getName().split("@")[0]);
        System.out.println(analyz("4022255165441838", "null", "我参与了@媒体人张晓磊 发起的投票 【男生都喜欢哪位明星？】，我投给了“刘涛”这个选项。你也快来表态吧~", "男生都喜欢这样的姑娘：刘诗诗、赵丽颖、郑爽、刘涛、高圆圆、赵雅芝、舒淇、林心如、杨紫、王子文、蒋欣、宋茜、徐冬冬、关晓彤、迪丽热巴、古力娜扎、唐嫣、杨幂、Angelababy，欧阳娜娜等，哪个姑娘才是你喜欢的类型？快来告诉大家吧！[心]我发起了一个投票 【男生都喜欢哪位明星？】...全文： http://m.weibo.cn/1283431191/4022255165441838 "));

        System.out.println("[PROGRAM] Program exited.");
    }
}

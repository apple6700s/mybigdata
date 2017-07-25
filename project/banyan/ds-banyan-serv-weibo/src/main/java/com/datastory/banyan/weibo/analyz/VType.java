package com.datastory.banyan.weibo.analyz;

/**
 * @author sezina
 * @since 9/9/16
 */
public enum VType {

    COMMON("普通用户", 0),
    BLUE("蓝v", 1),
    YELLOW("黄v", 2),
    DAREN("微博达人", 3);

    private String name;
    private int type;

    private VType(String name, int type) {
        this.name = name;
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public int getType() {
        return type;
    }

    public static VType fromName(String name) {
        if (name.equals(COMMON.getName()))
            return COMMON;
        else if (name.equals(DAREN.getName()))
            return DAREN;
        else if (name.equals(YELLOW.getName()))
            return YELLOW;
        else if (name.equals(BLUE.getName()))
            return BLUE;
        else
            return null;
    }

    public static VType fromType(int type) {
        if (type == COMMON.type)
            return COMMON;
        else if (type == DAREN.type)
            return DAREN;
        else if (type == YELLOW.type)
            return YELLOW;
        else if (type == BLUE.type)
            return BLUE;
        else
            return null;
    }

    /**
     * from original verifiedType from sina
     * <p/>
     * -1普通用户, 0名人, 1政府, 2企业, 3媒体, 4校园, 5网站, 6应用, 7团体（机构）, 8待审企业, 200初级达人, 220中高级达人, 400已故V用户。
     *
     * @param type
     * @return
     */
    public static VType fromOriginType(Integer type) {
        if (type == null)
            return VType.COMMON;
        else if (type == -1) {
            return VType.COMMON;
        } else if (type == 0 || type == 400) {
            return VType.YELLOW;
        } else if (type == 200 || type == 220) {
            return VType.DAREN;
        } else if (type >= 1 && type <= 8) {
            return VType.BLUE;
        } else {
            return VType.COMMON;
        }
    }

    public static VType fromOriginTypeStr(String typeStr) {
        try {
            Integer type = Integer.parseInt(typeStr);
            return fromOriginType(type);
        } catch (Exception e) {
            return fromOriginType(null);
        }
    }
}

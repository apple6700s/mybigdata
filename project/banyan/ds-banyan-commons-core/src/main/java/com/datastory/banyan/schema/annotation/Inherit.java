package com.datastory.banyan.schema.annotation;

import java.lang.annotation.*;

/**
 * com.datatub.banyan.commons.schema.annotation.Ch
 * 处理逻辑先include，后exclude。不填include表示include全部，不填exclude表示不管。
 * @author lhfcws
 * @since 2017/5/19
 */
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface Inherit {
    String include() default "*";
    String exclude() default "";
}

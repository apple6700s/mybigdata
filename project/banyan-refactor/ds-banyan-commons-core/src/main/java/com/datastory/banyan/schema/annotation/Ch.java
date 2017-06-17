package com.datastory.banyan.schema.annotation;

import java.lang.annotation.*;

/**
 * com.datatub.banyan.commons.schema.annotation.Ch
 *
 * @author lhfcws
 * @since 2017/5/19
 */
@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface Ch {
    String ch() default "";
    String cmt() default "";
}

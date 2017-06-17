package com.datastory.banyan.schema.annotation;

import java.lang.annotation.*;

/**
 * com.datastory.banyan.schema.annotation.Type
 *
 * @author lhfcws
 * @since 2017/5/26
 */
@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface Type {
    Class type() default String.class;
}

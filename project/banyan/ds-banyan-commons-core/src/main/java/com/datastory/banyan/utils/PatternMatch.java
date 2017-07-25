package com.datastory.banyan.utils;

import java.io.Serializable;

/**
 * com.datastory.banyan.utils.PatternMatch
 * 模拟模式匹配
 *
 * @author lhfcws
 * @since 16/11/24
 */

public class PatternMatch implements Serializable {

    public static <A> MatchFunc1<A> create(Class<A> klassA, MatchFunc1<A> func) {
        return func;
    }

    public static <A, B> MatchFunc2<A, B> create(Class<A> klassA, Class<B> klassB, MatchFunc2<A, B> func) {
        return func;
    }

    public static <A, B, C> MatchFunc3<A, B, C> create(Class<A> klassA, Class<B> klassB, Class<C> klassC, MatchFunc3<A, B, C> func) {
        return func;
    }

    public static <A, B, C, D> MatchFunc4<A, B, C, D> create(Class<A> klassA, Class<B> klassB, Class<C> klassC, Class<D> klassD, MatchFunc4<A, B, C, D> func) {
        return func;
    }

    /*******************************
     * Match function
     */
    private static interface _MatchFunc {
    }

    public static abstract class MatchFunc1<A> implements _MatchFunc {
        public void apply(Object in1) {
            try {
                _apply((A) in1);
            } catch (ClassCastException ignore) {
            }
        }

        public abstract void _apply(A in1);
    }

    public static abstract class MatchFunc2<A, B> implements _MatchFunc {
        public void apply(Object in1, Object in2) {
            try {
                _apply((A) in1, (B) in2);
            } catch (ClassCastException ignore) {
            }
        }

        public abstract void _apply(A in1, B in2);
    }

    public static abstract class MatchFunc3<A, B, C> implements _MatchFunc {
        public void apply(Object in1, Object in2, Object in3) {
            try {
                _apply((A) in1, (B) in2, (C) in3);
            } catch (ClassCastException ignore) {
            }
        }

        public abstract void _apply(A in1, B in2, C in3);
    }

    public static abstract class MatchFunc4<A, B, C, D> implements _MatchFunc {
        public void apply(Object in1, Object in2, Object in3, Object in4) {
            try {
                _apply((A) in1, (B) in2, (C) in3, (D) in4);
            } catch (ClassCastException ignore) {
            }
        }

        public abstract void _apply(A in1, B in2, C in3, D in4);
    }
}

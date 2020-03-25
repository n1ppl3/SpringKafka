package ru.n1ppl3.utils;

import java.util.Collections;
import java.util.List;

import static java.util.Arrays.asList;


public abstract class CollectionUtils {

    @SafeVarargs // possible heap pollution from parameterized vararg type
    public static <T extends Comparable<? super T>> List<T> sortedList(T... list) {
        return sortedList(asList(list));
    }

    public static <T extends Comparable<? super T>> List<T> sortedList(List<T> list) {
        Collections.sort(list);
        return list;
    }
}

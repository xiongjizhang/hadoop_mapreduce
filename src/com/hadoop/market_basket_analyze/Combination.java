package com.hadoop.market_basket_analyze;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Created by zhao on 2018-03-01.
 */
public class Combination {

    public static <T extends Comparable<? super T>> List<List<T>> findSortedCombinations(Collection<T> elements, int n) {
        List<List<T>> result = new ArrayList<List<T>>();
        if (n == 0) {
            result.add(new ArrayList<T>());
            return result;
        }

        List<List<T>> combinations = findSortedCombinations(elements, n-1);
        for (List<T> combination : combinations) {
            for (T element : elements) {
                if (combination.contains(element)) {
                    continue;
                }

                List<T> list = new ArrayList<T>();
                list.addAll(combination);
                list.add(element);

                Collections.sort(list);
                if (result.contains(list)) {
                    continue;
                }
                result.add(list);
            }
        }
        return result;
    }

}

package me.spb.navsi.random;

import java.util.Map;
import java.util.Random;
import java.util.function.BiFunction;

public class Utils {
    public static void incrementByKey(int key, Map<Integer, Integer> map) {
        map.compute(key, (k, oldValue) -> {
            if (oldValue == null)
                return 1;
            return oldValue + 1;
        });
    }

    public static int randInt(int min, int max) {
        Random rand = new Random();
        return rand.nextInt((max - min) + 1) + min;
    }
}

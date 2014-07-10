package org.opendaylight.controller.routing.dijkstrav2_implementation.internal;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author aanm
 */
public class HashMapUtils {

    public static <K, V> void putMultiValue(HashMap<K, List<V>> hm, K key, V value) {
        if (!hm.containsKey(key)) {
            hm.put(key, new ArrayList<V>());
        }
        hm.get(key).add(value);
    }

    /**
     * Adds all the keys and respective values from hmToInsert and inserts them
     * into hm.
     *
     * @param <K> The type of keys of HashMap.
     * @param <V> The type of values of HashMap.
     * @param hm The HashMap to insert the new values.
     * @param hmToInsert The new values to be inserted.
     */
    public static <K, V> void putAllMultiValue(HashMap<K, List<V>> hm, HashMap<K, List<V>> hmToInsert) {
        for (Map.Entry<K, List<V>> keysMap : hmToInsert.entrySet()) {
            K keys = keysMap.getKey();
            if (!hm.containsKey(keys)) {
                hm.put(keys, new ArrayList<V>());
            }
            List<V> listOfKey = keysMap.getValue();
            hm.get(keys).addAll(listOfKey);
        }
    }
}

package com.chen;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import com.google.common.hash.Hashing;

public class Utils {
    public static List<Long> myHash(String key, int k, int range) {
        List<Long> hashValues = new ArrayList<>();

        // Iterate over k hash functions
        for (int i = 0; i < k; i++) {
            // Calculate hash using MurmurHash algorithm
            long hash = Hashing.murmur3_128(i).hashString(key, StandardCharsets.UTF_8).asLong();

            // Map hash value to the specified range
            long mappedHash = Math.abs(hash) % range;

            hashValues.add(mappedHash);
        }

        return hashValues;
    }
}

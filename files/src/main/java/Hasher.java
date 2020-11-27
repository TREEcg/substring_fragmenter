package main.java;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class Hasher {
    protected final HashFunction hasher;

    Hasher() {
        this.hasher = Hashing.goodFastHash(64); // used to hash prefixes
    }

    public long hash(List<String> values) {
        java.util.Collections.sort(values);
        long result = 0;
        for ( String s : values) {
            result += this.hasher.hashString(s, StandardCharsets.UTF_8).asLong();
        }
        return result;
    }
}

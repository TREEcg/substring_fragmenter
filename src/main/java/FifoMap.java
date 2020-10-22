package main.java;

import org.apache.jena.riot.system.StreamRDF;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class FifoMap<K> extends LinkedHashMap<K, StreamRDF> {
    int max;

    public FifoMap (int max){
        super(max + 1);
        this.max = max;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<K, StreamRDF> entry) {
        if (size() > this.max) {
            this.values().stream().findFirst().get().finish();
            return true;
        }

        return false;
    }
}

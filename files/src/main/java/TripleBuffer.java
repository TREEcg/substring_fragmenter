package main.java;

import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.Quad;

import java.util.*;

class TripleBuffer {
    protected final String subject;
    protected final Set<Triple> triples;
    protected final Set<Quad> quads;

    TripleBuffer(String subject) {
        this.subject = subject;
        this.triples = new HashSet<>();
        this.quads = new HashSet<>();
    }

    public void addTriple(Triple triple) {
        this.triples.add(triple);
    }

    public void addQuad(Quad quad) {
        this.quads.add(quad);
    }

    public Set<Triple> getTriples() {
        return this.triples;
    }

    public Set<Quad> getQuads() {
        return this.quads;
    }
}
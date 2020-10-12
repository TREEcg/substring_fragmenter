package com.company;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.apache.jena.atlas.lib.CharSpace;
import org.apache.jena.datatypes.TypeMapper;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.*;
import org.apache.jena.riot.out.NodeFormatter;
import org.apache.jena.riot.out.NodeFormatterNT;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.system.StreamRDFLib;
import org.apache.jena.sparql.core.Quad;

import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.*;

@SuppressWarnings("UnstableApiUsage")
class FragmentSink implements StreamRDF {
    protected final Node[] properties;
    protected final Map<Long, StreamRDF> outStreams;
    protected final Map<Long, Integer> counts;
    protected final NodeFormatter nodeFmt;
    protected final HashFunction hasher;
    protected final Path outDirPath;

    FragmentSink(ArrayList<Property> properties, int maxFileHandles, Path outDirPath) {
        this.nodeFmt = new NodeFormatterNT(CharSpace.UTF8);
        this.outStreams = new FifoMap<>(maxFileHandles);
        this.counts = new TreeMap<>();
        this.properties = new Node[properties.size()];
        this.outDirPath = outDirPath;
        this.hasher = Hashing.goodFastHash(64);
        for ( int i = 0 ; i < properties.size() ; i++ ) {
            this.properties[i] = properties.get(i).asNode();
        }
    }

    public void addHypermedia(URI domain) throws IOException {
        Deque<String> queue = new LinkedList<>();
        queue.add("");

        Node viewPredicate = NodeFactory.createURI("https://w3id.org/tree#view");
        Node relationPredicate = NodeFactory.createURI("https://w3id.org/tree#relation");
        Node typePredicate = NodeFactory.createURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
        Node nodePredicate = NodeFactory.createURI("https://w3id.org/tree#node");
        Node valuePredicate = NodeFactory.createURI("https://w3id.org/tree#value");
        Node remainingPredicate = NodeFactory.createURI("https://w3id.org/tree#remainingItems");
        Node relationObject = NodeFactory.createURI("https://w3id.org/tree#PrefixRelation");
        Node rootNode = NodeFactory.createURI(domain.toASCIIString());

        while (queue.size() > 0) {
            String current = queue.pop();
            Path filePath;
            Node thisNode;
            if (current.length() == 0) {
                filePath = this.outDirPath.resolve(".root.nt");
                thisNode = NodeFactory.createURI(domain.toASCIIString());
            } else {
                filePath = this.outDirPath.resolve(current + ".nt");
                thisNode = NodeFactory.createURI(domain.resolve("/" + current).toASCIIString());
            }

            StreamRDF out = StreamRDFLib.writer(new FileWriter(String.valueOf(filePath), true));
            Triple viewStatement = Triple.create(rootNode, viewPredicate, thisNode);
            out.triple(viewStatement);

            for(char alphabet = 'a'; alphabet <='z'; alphabet++ ) {
                String next = current + alphabet;
                long hash = this.hash(next);
                if (this.counts.containsKey(hash)) {
                    queue.add(next);
                    int count = this.counts.get(hash);

                    Node nextNode = NodeFactory.createURI(domain.resolve("/" + next).toASCIIString());
                    Node nextValue = NodeFactory.createLiteral(next);
                    Node remainingNode = NodeFactory.createLiteralByValue(count, TypeMapper.getInstance().getTypeByValue(count));

                    Node relationNode = NodeFactory.createBlankNode(next);
                    out.triple(Triple.create(thisNode, relationPredicate, relationNode));
                    out.triple(Triple.create(relationNode, typePredicate, relationObject));
                    out.triple(Triple.create(relationNode, nodePredicate, nextNode));
                    out.triple(Triple.create(relationNode, valuePredicate, nextValue));
                    out.triple(Triple.create(nextNode, remainingPredicate, remainingNode));
                }
            }

            out.finish();
        }

    }

    public void start() {
    }

    public void finish() {
        for (StreamRDF out : this.outStreams.values()) {
            out.finish();
        }
    }

    public void triple(Triple triple) {
        for ( Node p : properties ) {
            if ( triple.getPredicate().equals(p) ) {
                for (StreamRDF out : this.getOutStreams(triple.getObject().getLiteralLexicalForm())) {
                    out.triple(triple);
                }
            }
        }
    }

    public void quad(Quad quad) {
        for ( Node p : properties ) {
            if ( quad.getPredicate().equals(p) ) {
                for (StreamRDF out : this.getOutStreams(quad.getObject().getLiteralLexicalForm())) {
                    out.quad(quad);
                }
            }
        }
    }

    public void base(String base) {
    }

    public void prefix(String prefix, String iri) {
    }

    private Iterable<StreamRDF> getOutStreams(String s)  {
        ArrayList<StreamRDF> result = new ArrayList<>();
        for (String token : this.tokenize(s)) {
            String cleanToken = token.replaceAll("[^a-zA-Z]", "");
            for (String prefix : this.prefixes(cleanToken)) {
                Long hash = this.hash(prefix);
                if (!this.counts.containsKey(hash)) {
                    this.counts.put(hash, 0);
                }

                int count = this.counts.get(hash);
                this.counts.put(hash, count + 1);
                if (count < 100 || token.length() - prefix.length() < 2) {
                    result.add(this.getOutStream(prefix, hash));
                } else {
                    break;
                }
            }
        }
        return result;
    }

    private long hash(String s) {
        return this.hasher.hashString(s, StandardCharsets.UTF_8).asLong();
    }

    private StreamRDF getOutStream(String prefix, Long hash)  {
        if (!this.outStreams.containsKey(hash)) {
            Path filePath = this.outDirPath.resolve(prefix + ".nt");
            //this.outStreams.put(hash, StreamRDFLib.writer(OutputStream.nullOutputStream()));
            try {
                this.outStreams.put(hash, StreamRDFLib.writer(new FileWriter(String.valueOf(filePath), true)));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return this.outStreams.get(hash);
    }

    private Iterable<String> tokenize(String value) {
        String[] parts = value.split(" ");
        List<String> result = new ArrayList<>();
        for (String p : parts) {
            if (p.length() > 2) {
                result.add(p);
            }
        }
        if (result.isEmpty()) {
            return Arrays.asList(parts);
        } else {
            return result;
        }
    }

    private Iterable<String> prefixes(String value) {
        ArrayList<String> result = new ArrayList<>();
        for (int i = value.length(); i > 0; i--) {
            result.add(value.substring(0, i).toLowerCase());
        }
        return result;
    }
}
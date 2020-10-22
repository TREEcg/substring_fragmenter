package main.java;

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
    protected final Map<Long, Integer> written;
    protected final NodeFormatter nodeFmt;
    protected final HashFunction hasher;
    protected final Path outDirPath;

    FragmentSink(ArrayList<Property> properties, int maxFileHandles, Path outDirPath) {
        this.nodeFmt = new NodeFormatterNT(CharSpace.UTF8);
        this.outStreams = new FifoMap<>(maxFileHandles);
        this.counts = new HashMap<>();
        this.written = new HashMap<>();
        this.properties = new Node[properties.size()];
        this.outDirPath = outDirPath;
        this.hasher = Hashing.goodFastHash(64);
        for ( int i = 0 ; i < properties.size() ; i++ ) {
            this.properties[i] = properties.get(i).asNode();
        }
    }

    public void addHypermedia(URI root) throws IOException {
        // define some Node objects we'll need
        Node subsetPredicate = NodeFactory.createURI("http://rdfs.org/ns/void#subset");
        Node relationPredicate = NodeFactory.createURI("https://w3id.org/tree#relation");
        Node typePredicate = NodeFactory.createURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
        Node nodePredicate = NodeFactory.createURI("https://w3id.org/tree#node");
        Node valuePredicate = NodeFactory.createURI("https://w3id.org/tree#value");
        Node remainingPredicate = NodeFactory.createURI("https://w3id.org/tree#remainingItems");
        Node treePathPredicate = NodeFactory.createURI("https://w3id.org/tree#path");
        Node treeShapePredicate = NodeFactory.createURI("https://w3id.org/tree#shape");
        Node shaclPropertyPredicate = NodeFactory.createURI("https://www.w3.org/ns/shacl#alternativePath");
        Node shaclPathPredicate = NodeFactory.createURI("https://www.w3.org/ns/shacl#path");
        Node shaclMinCountPredicate = NodeFactory.createURI("https://www.w3.org/ns/shacl#minCount");
        Node alternatePathPredicate = NodeFactory.createURI("https://www.w3.org/ns/shacl#alternativePath");
        Node relationObject = NodeFactory.createURI("https://w3id.org/tree#PrefixRelation");
        Node rootNode = NodeFactory.createURI(root.toASCIIString());

        // memorizing all prefixes requires an impossible amount of memory
        // we do store all hashed prefixes however, but this is not reversible
        // so we enumerate 'all' prefixes again,
        // using the hashed prefixes to verify whether or not a prefix exists in the dataset
        Deque<String> queue = new LinkedList<>();
        queue.add("");

        while (queue.size() > 0) {
            String current = queue.pop();
            Path filePath;
            Node thisNode;
            if (current.length() == 0) {
                filePath = this.outDirPath.resolve(".root.nt");
                thisNode = NodeFactory.createURI(root.toASCIIString());
            } else {
                filePath = this.outDirPath.resolve(current + ".nt");
                thisNode = NodeFactory.createURI(root.resolve("./" + current).toASCIIString());
            }

            // add hypermedia controls to all non-leaf nodes
            long currentHash = this.hash(current);
            if (current.length() == 0 || this.counts.get(currentHash) > 100) {
                StreamRDF out = StreamRDFLib.writer(new FileWriter(String.valueOf(filePath), true));

                // define this page as a subset of the collection as a whole
                out.triple(Triple.create(rootNode, subsetPredicate, thisNode));

                // create a shacl path object that defines which properties are contained in this dataset
                Node pathNode;
                if (this.properties.length > 1) {
                    pathNode = NodeFactory.createBlankNode("path_node");
                    for (Node propertyNode : this.properties) {
                        out.triple(Triple.create(pathNode, alternatePathPredicate, propertyNode));
                    }
                } else {
                    pathNode = this.properties[0];
                }

                // link the previously-defined shacl path to the dataset
                // this communicates to clients which data can be found here
                Node shapeNode = NodeFactory.createBlankNode("shape_node");
                out.triple(Triple.create(rootNode, treeShapePredicate, shapeNode));
                Node propertyNode = NodeFactory.createBlankNode("property_node");
                out.triple(Triple.create(shapeNode, shaclPropertyPredicate, propertyNode));
                out.triple(Triple.create(propertyNode, shaclPathPredicate, pathNode));
                int temp = 1;
                Node tempNode = NodeFactory.createLiteralByValue(temp, TypeMapper.getInstance().getTypeByValue(temp));
                out.triple(Triple.create(propertyNode, shaclMinCountPredicate, tempNode));

                // add links to the following data pages
                // note that just enumerating over the alphabet is a naive solution
                for(char alphabet = 'a'; alphabet <='z'; alphabet++ ) {
                    String next = current + alphabet;
                    long nextHash = this.hash(next);

                    // checking the hash is faster than checking the file's existence - but may backfire
                    if (this.counts.containsKey(nextHash)) {
                        queue.add(next);
                        int count = this.counts.get(nextHash);

                        Node nextNode = NodeFactory.createURI(root.resolve("./" + next).toASCIIString());
                        Node nextValue = NodeFactory.createLiteral(next);
                        Node remainingNode = NodeFactory.createLiteralByValue(count, TypeMapper.getInstance().getTypeByValue(count));

                        Node relationNode = NodeFactory.createBlankNode(next);
                        out.triple(Triple.create(thisNode, relationPredicate, relationNode));
                        out.triple(Triple.create(relationNode, typePredicate, relationObject));
                        out.triple(Triple.create(relationNode, nodePredicate, nextNode));
                        out.triple(Triple.create(relationNode, valuePredicate, nextValue));
                        out.triple(Triple.create(relationNode, treePathPredicate, pathNode));
                        out.triple(Triple.create(nextNode, remainingPredicate, remainingNode));
                    }
                }

                out.finish();
            }
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
                    this.written.put(hash, 0);
                }

                int written = this.written.get(hash);
                int count = this.counts.get(hash);
                this.counts.put(hash, count + 1);

                boolean write = false;
                write |= written < 100;
                write |= (written < 200 && token.length() - prefix.length() < 2);
                write |= (written < 300 && token.length() - prefix.length() < 1);
                write |= (written < 400 && token.length() == prefix.length());
                write |= (written < 800 && token.length() == prefix.length() && token.length() > 4);

                if (write) {
                    result.add(this.getOutStream(prefix, hash));
                    this.written.put(hash, written + 1);
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
        return Arrays.asList(value.split(" "));
        /*
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
        */
    }

    private Iterable<String> prefixes(String value) {
        ArrayList<String> result = new ArrayList<>();
        for (int i = value.length(); i > 0; i--) {
            result.add(value.substring(0, i).toLowerCase());
        }
        return result;
    }
}
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
import java.text.Normalizer;
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
    protected final Set<Character> charSet;

    protected Optional<TripleBuffer> buffer;

    FragmentSink(ArrayList<Property> properties, int maxFileHandles, Path outDirPath) {
        this.nodeFmt = new NodeFormatterNT(CharSpace.UTF8); // creates ntriples lines
        this.outStreams = new FifoMap<>(maxFileHandles);    // all open file handles
        this.counts = new HashMap<>();  // how often a prefix was encountered
        this.written = new HashMap<>(); // how often a prefix (fragment) was written to

        // some implementation/optimization details
        this.hasher = Hashing.goodFastHash(64); // used to hash prefixes
        this.charSet = new HashSet<>(); // used to enumerate all possible prefixes when creating hypermedia links

        this.outDirPath = outDirPath; // root location to write to

        // stuff to filter on
        this.properties = new Node[properties.size()];
        for ( int i = 0 ; i < properties.size() ; i++ ) {
            this.properties[i] = properties.get(i).asNode();
        }

        this.buffer = Optional.empty();
    }

    public void flush(TripleBuffer buffer) {
        Set<String> values = new HashSet<>();

        for ( Node p : properties ) {
            for ( Triple triple : buffer.getTriples()) {
                if (triple.getPredicate().getURI().equals(p.getURI())) {
                    values.add(triple.getObject().getLiteralLexicalForm());
                }
            }
            for ( Quad quad : buffer.getQuads()) {
                if (quad.getPredicate().getURI().equals(p.getURI())) {
                    values.add(quad.getObject().getLiteralLexicalForm());
                }
            }
        }

        for (StreamRDF out : this.getOutStreams(values)) {
            for ( Triple triple : buffer.getTriples()) {
                out.triple(triple);
            }
            for ( Quad quad : buffer.getQuads()) {
                out.quad(quad);
            }
        }
    }

    @Override
    public void triple(Triple triple) {
        if (triple.getSubject().isURI()) {
            String subject = triple.getSubject().getURI();

            if (this.buffer.isPresent()) {
                TripleBuffer buffer = this.buffer.get();
                if (buffer.subject != subject) {
                    this.flush(buffer);
                    this.buffer = Optional.of(new TripleBuffer(subject));
                } else {
                    int i = 9;
                }
            } else {
                this.buffer = Optional.of(new TripleBuffer(subject));
            }

            this.buffer.get().addTriple(triple);
        }
    }

    @Override
    public void quad(Quad quad) {
        if (quad.getSubject().isURI()) {
            String subject = quad.getSubject().getURI();

            if (this.buffer.isPresent()) {
                TripleBuffer buffer = this.buffer.get();
                if (buffer.subject != subject) {
                    this.flush(buffer);
                    this.buffer = Optional.of(new TripleBuffer(subject));
                }
            } else {
                this.buffer = Optional.of(new TripleBuffer(subject));
            }

            this.buffer.get().addQuad(quad);
        }
    }

    @Override
    public void finish() {
        if (this.buffer.isPresent()) {
            this.flush(this.buffer.get());
        }

        // flush all open file handles
        for (StreamRDF out : this.outStreams.values()) {
            out.finish();
        }
    }

    public List<List<String>> expandTokens(List<String> given) {
        List<List<String>> result = new ArrayList<>();
        for ( int i = 0; i < given.size(); i++) {
            for(char c : this.charSet ) {
                String replacementToken = given.get(i) + c;
                List<String> tokens = new ArrayList<>(given);
                tokens.set(i, replacementToken);
                result.add(tokens);
            }
        }
        for (char c:this.charSet) {
            List<String> tokens = new ArrayList<>(given);
            tokens.add("" + c);
            result.add(tokens);
        }
        return result;
    }

    public void addHypermedia(URI root) throws IOException {
        // define some Node objects we'll need
        // this is probably not the most idiomatic way
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
        Node relationObject = NodeFactory.createURI("https://w3id.org/tree#SubstringRelation");
        Node rootNode = NodeFactory.createURI(root.toASCIIString());

        // memorizing all prefixes requires an impossible amount of memory
        // so we store all hashed prefixes, but this is not reversible
        // we enumerate 'all' prefixes instead, using the counts to verify each prefix's existence
        Deque<List<String>> queue = new LinkedList<>();
        List<String> start = new ArrayList<>();
        queue.add(start);

        // for logging purposes, remember the largest fragment
        int mostWrittenCount = -1;
        List<String> mostWrittenPrefix = new ArrayList<>();

        while (queue.size() > 0) {
            List<String> current = queue.pop();
            Path filePath;
            Node thisNode;

            // root node is hard coded
            if (current.size() == 0) {
                filePath = this.outDirPath.resolve(".root.nt");
                thisNode = NodeFactory.createURI(root.toASCIIString());
            } else {
                String identifier = String.join("+", current);
                filePath = this.outDirPath.resolve(identifier + ".nt");
                thisNode = NodeFactory.createURI(root.resolve("./" + identifier).toASCIIString());
            }

            // add hypermedia controls to all non-leaf nodes
            long currentHash = this.hash(current);
            if (this.written.containsKey(currentHash)) {
                int currentWrittenCount = this.written.get(currentHash);
                if (mostWrittenCount < 0 || currentWrittenCount > mostWrittenCount) {
                    mostWrittenCount = currentWrittenCount;
                    mostWrittenPrefix = current;
                }
            }

            if (current.size() == 0 || this.counts.get(currentHash) > 100) {
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
                // by just iterating over all known possible prefix extensions
                for( List<String> next : this.expandTokens(current) ) {
                    long nextHash = this.hash(next);

                    // checking the hash is faster than checking the file's existence - but may backfire
                    if (this.counts.containsKey(nextHash)) {
                        queue.add(next);
                        int count = this.counts.get(nextHash);

                        Node nextNode = NodeFactory.createURI(root.resolve("./" + String.join("+", next)).toASCIIString());
                        Node remainingNode = NodeFactory.createLiteralByValue(count, TypeMapper.getInstance().getTypeByValue(count));

                        Node relationNode = NodeFactory.createBlankNode(String.join("+", current));
                        out.triple(Triple.create(thisNode, relationPredicate, relationNode));
                        out.triple(Triple.create(relationNode, typePredicate, relationObject));
                        out.triple(Triple.create(relationNode, nodePredicate, nextNode));
                        for (String token : next) {
                            Node tokenValue = NodeFactory.createLiteral(token);
                            out.triple(Triple.create(relationNode, valuePredicate, tokenValue));
                        }
                        out.triple(Triple.create(relationNode, treePathPredicate, pathNode));
                        out.triple(Triple.create(nextNode, remainingPredicate, remainingNode));
                    }
                }

                out.finish();
            }
        }

        System.out.println("Fullest page: " + mostWrittenPrefix + " @ " + mostWrittenCount);
    }

    private Iterable<StreamRDF> getOutStreams(Iterable<String> values)  {
        Set<List<String>> substringSet = new HashSet<>();

        // remove diacritics
        for ( String s : values) {
            String cleanString = this.normalize(s);

            // memorize all used characters so we can later piece together the hypermedia controls
            this.registerCharacters(cleanString);

            boolean success = false;
            for (String token : this.tokenize(cleanString)) {
                for (String prefix : this.prefixes(token)) {
                    List<String> tokens = new ArrayList<>();
                    tokens.add(prefix);

                    Long hash = this.hash(tokens);
                    if (!this.counts.containsKey(hash)) {
                        this.counts.put(hash, 0);
                        this.written.put(hash, 0);
                    }

                    int written = this.written.get(hash);
                    int count = this.counts.get(hash);
                    this.counts.put(hash, count + 1);

                    // naive approach to avoiding overfull fragments
                    // we aim at 400-800 triples per fragment
                    boolean write = written < 400;
                    write |= (written < 500 && token.length() - prefix.length() < 2);
                    write |= (written < 600 && token.length() - prefix.length() < 1);
                    write |= (written < 700 && token.length() == prefix.length());
                    write |= (written < 800 && token.length() == prefix.length() && token.length() > 4);

                    if (write) {
                        success = true; // used to ensure every triple goes somewhere
                        substringSet.add(tokens);
                        //this.written.put(hash, written + 1);
                    }
                }
            }

            if (!success) {
                // we haven't written this triple anywhere yet; write it to the emptiest page
                int lowestCount = -1;
                String bestPrefix = "";

                for (String token : this.tokenize(cleanString)) {
                    for (String prefix : this.prefixes(token)) {
                        List<String> tokens = new ArrayList<>();
                        tokens.add(prefix);
                        Long hash = this.hash(tokens);
                        int written = this.written.get(hash);

                        if (lowestCount < 0 || written < lowestCount) {
                            lowestCount = written;
                            bestPrefix = prefix;
                        }
                    }
                }

                List<String> tokens = new ArrayList<>();
                tokens.add(bestPrefix);
                substringSet.add(tokens);
            }
        }

        ArrayList<StreamRDF> result = new ArrayList<>();

        for ( List<String> tokens : substringSet ) {
            Long hash = this.hash(tokens);
            result.add(this.getOutStream(tokens, hash));
            int written = this.written.get(hash);
            this.written.put(hash, written + 1);
        }

        return result;
    }

    private void registerCharacters(String s) {
        for (char c : s.toCharArray()) {
            this.charSet.add(c);
        }
    }

    private long hash(List<String> values) {
        java.util.Collections.sort(values);
        long result = 0;
        for ( String s : values) {
            result += this.hasher.hashString(s, StandardCharsets.UTF_8).asLong();
        }
        return result;
    }

    private StreamRDF getOutStream(List<String> tokens, Long hash)  {
        if (!this.outStreams.containsKey(hash)) {
            java.util.Collections.sort(tokens);
            Path filePath = this.outDirPath.resolve(String.join("+", tokens) + ".nt");
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
    }

    private String normalize(String original) {
        String reduced = original.toLowerCase();

        // normalize unicode string
        // see http://www.unicode.org/reports/tr15/#Normalization_Forms_Table
        reduced = Normalizer.normalize(reduced, Normalizer.Form.NFKD);

        // discard all Mark values
        // see http://www.unicode.org/reports/tr44/#General_Category_Values
        reduced = reduced.replaceAll("\\p{M}", "");

        // retain all letters/digits/whitespace
        reduced = reduced.replaceAll("[^\\p{IsDigit}\\p{IsLetter}\\p{IsIdeographic}\\p{javaSpaceChar}]", "");
        return reduced;
    }

    private Iterable<String> prefixes(String value) {
        ArrayList<String> result = new ArrayList<>();
        for (int i = value.length(); i > 0; i--) {
            result.add(value.substring(0, i));
        }
        return result;
    }

    public void base(String base) {

    }

    public void prefix(String prefix, String iri) {

    }

    public void start() {

    }
}
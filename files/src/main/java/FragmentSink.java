package main.java;

import org.apache.jena.atlas.lib.CharSpace;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.out.NodeFormatter;
import org.apache.jena.riot.out.NodeFormatterNT;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.system.StreamRDFLib;
import org.apache.jena.sparql.core.Quad;

import javax.annotation.Nullable;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.file.Path;
import java.text.Normalizer;
import java.util.*;

import static java.lang.System.exit;

class FragmentSink implements StreamRDF {
    protected final List<Node> properties;
    protected final Map<Long, StreamRDF> outStreams;
    protected final Map<Long, Integer> counts;
    protected final Map<Long, Integer> written;
    protected final NodeFormatter nodeFmt;
    protected final Path outDirPath;
    protected final Set<Character> charSet;
    protected final Hasher hasher;

    @Nullable
    protected TripleBuffer buffer;

    FragmentSink(List<Node> properties, int maxFileHandles, Path outDirPath, Hasher hasher) {
        this.hasher = hasher;
        this.nodeFmt = new NodeFormatterNT(CharSpace.UTF8); // creates ntriples lines
        this.outStreams = new FifoMap<>(maxFileHandles);    // all open file handles
        this.counts = new HashMap<>();  // how often a prefix was encountered
        this.written = new HashMap<>(); // how often a prefix (fragment) was written to

        // some implementation/optimization details
        this.charSet = new HashSet<>(); // used to enumerate all possible prefixes when creating hypermedia links

        this.outDirPath = outDirPath; // root location to write to

        // stuff to filter on
        this.properties = properties;
        this.buffer = null;
    }

    public Map<Long, Integer> getCounts() {
        return counts;
    }

    public Map<Long, Integer> getWritten() {
        return written;
    }

    public Set<Character> getCharSet() {
        return charSet;
    }

    public void flush() {
        if (this.buffer != null) {
            Set<String> values = new HashSet<>();
            for ( Node p : properties ) {
                for ( Triple triple : this.buffer.getTriples()) {
                    if (triple.getPredicate().getURI().equals(p.getURI())) {
                        values.add(triple.getObject().getLiteralLexicalForm());
                    }
                }
                for ( Quad quad : this.buffer.getQuads()) {
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
    }

    @Override
    public void triple(Triple triple) {
        if (triple.getSubject().isURI()) {
            String subject = triple.getSubject().getURI();

            if (this.buffer != null) {
                if (!this.buffer.subject.equals(subject)) {
                    this.flush();
                    this.buffer = new TripleBuffer(subject);
                }
            } else {
                this.buffer = new TripleBuffer(subject);
            }

            this.buffer.addTriple(triple);
        }
    }

    @Override
    public void quad(Quad quad) {
        if (quad.getSubject().isURI()) {
            String subject = quad.getSubject().getURI();

            if (this.buffer != null) {
                if (!this.buffer.subject.equals(subject)) {
                    this.flush();
                    this.buffer = new TripleBuffer(subject);
                }
            } else {
                this.buffer = new TripleBuffer(subject);
            }

            this.buffer.addQuad(quad);
        }
    }

    @Override
    public void finish() {
        if (this.buffer != null) {
            this.flush();
        }

        // flush all open file handles
        for (StreamRDF out : this.outStreams.values()) {
            out.finish();
        }
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

                    Long hash = this.hasher.hash(tokens);
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
                        Long hash = this.hasher.hash(tokens);
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
            Long hash = this.hasher.hash(tokens);
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

    private StreamRDF getOutStream(List<String> tokens, Long hash)  {
        if (!this.outStreams.containsKey(hash)) {
            java.util.Collections.sort(tokens);
            Path filePath = this.outDirPath.resolve(String.join("+", tokens) + ".nt");
            try {
                OutputStreamWriter fileWriter = new FileWriter(String.valueOf(filePath), true);
                StreamRDF rdfWriter = StreamRDFLib.writer(fileWriter);
                this.outStreams.put(hash, rdfWriter);
            } catch (IOException e) {
                e.printStackTrace(System.out);
                exit(1);
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
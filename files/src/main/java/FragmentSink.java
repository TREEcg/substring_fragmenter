package main.java;

import org.apache.jena.atlas.lib.CharSpace;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.out.NodeFormatter;
import org.apache.jena.riot.out.NodeFormatterNT;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.system.StreamRDFWriter;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.util.PrintUtil;

import javax.annotation.Nullable;
import java.io.*;
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
    protected final String extension;

    @Nullable
    protected TripleBuffer buffer;

    FragmentSink(List<Node> properties, int maxFileHandles, Path outDirPath, Hasher hasher, String extension) {
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
        this.extension = extension;
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
                        String language = triple.getObject().getLiteralLanguage();
                        if (!(
                                language.equals("ja")
                                        || language.equals("zh")
                                        || language.equals("ko")
                                        || language.equals("zh-cn")
                                        || language.equals("got")
                                        || language.equals("yue")
                        )) {
                            values.add(triple.getObject().getLiteralLexicalForm());
                        }
                    }
                }
                for ( Quad quad : this.buffer.getQuads()) {
                    if (quad.getPredicate().getURI().equals(p.getURI()) && quad.getObject().isLiteral()) {
                        String language = quad.getObject().getLiteralLanguage();
                        if (!(
                            language.equals("ja")
                            || language.equals("zh")
                            || language.equals("ko")
                            || language.equals("zh-cn")
                            || language.equals("got")
                            || language.equals("yue")
                        )) {
                            values.add(quad.getObject().getLiteralLexicalForm());
                        }
                    }
                }
            }

            try {
                Iterable<StreamRDF> streams = this.getOutStreams(values);
                for (StreamRDF out : streams) {
                    for ( Triple triple : buffer.getTriples()) {
                        out.triple(triple);
                    }
                    for ( Quad quad : buffer.getQuads()) {
                        out.quad(quad);
                    }
                }
            } catch (IOException ex) {
                System.out.println("Error occurred while handling these statements:");
                for ( Triple triple : this.buffer.getTriples() ) {
                    PrintUtil.print(triple);
                }
                for ( Quad quad : this.buffer.getQuads() ) {
                    PrintUtil.print(quad);
                }
                System.out.println("The error:");
                ex.printStackTrace(System.out);
                exit(1);
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

    private List<Integer> startingPositions(String value) {
        boolean flagNext = true;
        List<Integer> result = new ArrayList<>();
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (!Character.isLetter(c) && !Character.isDigit(c)) {
                flagNext = true;
            } else {
                if (flagNext) {
                    flagNext = false;
                    result.add(i);
                }
            }
        }
        return result;
    }

    private Set<List<String>> selectSubstrings(String value) {
        Set<List<String>> substringSet = new HashSet<>();
        String valueValue = value + " " + value;

        outerLoop:
        for (Integer start : this.startingPositions(value)) {
            String currentSubstring = "";
            for (int i = start; i < start + value.length(); i++) {
                char newChar = valueValue.charAt(i);
                currentSubstring += newChar;

                if (newChar != ' ') {
                    List<String> tokens = Arrays.asList(currentSubstring.strip().split("[^\\p{IsDigit}\\p{IsLetter}]+"));
                    Long hash = this.hasher.hash(tokens);
                    if (!this.counts.containsKey(hash)) {
                        this.counts.put(hash, 0);
                        this.written.put(hash, 0);
                    }

                    int written = this.written.get(hash);
                    int count = this.counts.get(hash);
                    this.counts.put(hash, count + 1);

                    if (written < 100) {
                        this.written.put(hash, written + 1);
                        substringSet.add(tokens);
                        continue outerLoop;
                    }
                }
            }

            // Last resort, all tried fragments were full
            List<String> tokens = Arrays.asList(currentSubstring.split(" "));
            Long hash = this.hasher.hash(tokens);
            if (!this.counts.containsKey(hash)) {
                this.counts.put(hash, 0);
                this.written.put(hash, 0);
            }

            int written = this.written.get(hash);
            int count = this.counts.get(hash);
            this.counts.put(hash, count + 1);
            this.written.put(hash, written + 1);
            substringSet.add(tokens);
        }

        return substringSet;
    }

    private Iterable<StreamRDF> getOutStreams(Iterable<String> values) throws IOException {
        Set<List<String>> substringSet = new HashSet<>();

        // remove diacritics
        for ( String s : values) {
            String cleanString = this.normalize(s);

            // memorize all used characters so we can later piece together the hypermedia controls
            this.registerCharacters(cleanString);
            substringSet.addAll(this.selectSubstrings(cleanString));
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

    private StreamRDF getOutStream(List<String> tokens, Long hash) throws IOException {
        if (!this.outStreams.containsKey(hash)) {
            java.util.Collections.sort(tokens);

            Path filePath = this.outDirPath.resolve(String.join("+", tokens) + this.extension);
            OutputStream fileWriter = new FileOutputStream(String.valueOf(filePath), true);
            StreamRDF rdfWriter;
            if (this.extension == ".trig") {
                rdfWriter = StreamRDFWriter.getWriterStream(fileWriter, Lang.TRIG);
            } else {
                rdfWriter = StreamRDFWriter.getWriterStream(fileWriter, Lang.TURTLE);
            }
            this.outStreams.put(hash, rdfWriter);
        }

        return this.outStreams.get(hash);
    }

    private String normalize(String original) {
        String reduced = original.toLowerCase();

        // normalize unicode string
        // see http://www.unicode.org/reports/tr15/#Normalization_Forms_Table
        return Normalizer.normalize(reduced, Normalizer.Form.NFKD);
    }

    public void base(String base) {

    }

    public void prefix(String prefix, String iri) {

    }

    public void start() {

    }
}
package main.java;

import org.apache.jena.datatypes.TypeMapper;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.system.StreamRDFLib;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

public class HypermediaControls {
    protected final List<Node> properties;
    protected final Map<Long, Integer> counts;
    protected final Map<Long, Integer> written;
    protected final Path outDirPath;
    protected final Set<Character> charSet;
    protected final Hasher hasher;
    protected final String extension;

    HypermediaControls(
            List<Node> properties,
            Map<Long, Integer> counts,
            Map<Long, Integer> written,
            Hasher hasher,
            Path outDirPath,
            Set<Character> charSet,
            String extension
    ) {
        this.properties = properties;
        this.counts = counts;
        this.written = written;
        this.outDirPath = outDirPath; // root location to write to
        this.charSet = charSet;
        this.hasher = hasher;
        this.extension = extension;
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

    public void addHypermedia(String root) throws IOException {
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
        Node shaclPropertyPredicate = NodeFactory.createURI("http://www.w3.org/ns/shacl#alternativePath");
        Node shaclPathPredicate = NodeFactory.createURI("http://www.w3.org/ns/shacl#path");
        Node shaclMinCountPredicate = NodeFactory.createURI("http://www.w3.org/ns/shacl#minCount");
        Node alternatePathPredicate = NodeFactory.createURI("http://www.w3.org/ns/shacl#alternativePath");
        Node relationObject = NodeFactory.createURI("https://w3id.org/tree#SubstringRelation");
        Node shaclPatternPredicate = NodeFactory.createURI("http://www.w3.org/ns/shacl#pattern");
        Node shaclFlagsPredicate = NodeFactory.createURI("http://www.w3.org/ns/shacl#flags");
        Node rootNode = NodeFactory.createURI(root);

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
                filePath = this.outDirPath.resolve(".root" + this.extension);
                thisNode = NodeFactory.createURI(root);
            } else {
                String identifier = String.join("+", current);
                filePath = this.outDirPath.resolve(identifier + this.extension);
                thisNode = NodeFactory.createURI(root + identifier + this.extension);
            }

            // add hypermedia controls to all non-leaf nodes
            long currentHash = this.hasher.hash(current);
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
                if (this.properties.size() > 1) {
                    pathNode = NodeFactory.createBlankNode("path_node");
                    for (Node propertyNode : this.properties) {
                        out.triple(Triple.create(pathNode, alternatePathPredicate, propertyNode));
                    }
                } else {
                    pathNode = this.properties.get(0);
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

                Node patternNode = NodeFactory.createLiteral("[\\p{L}\\p{N}]+", "");
                Node flagsNode = NodeFactory.createLiteral("i");

                // add links to the following data pages
                // by just iterating over all known possible prefix extensions
                for( List<String> next : this.expandTokens(current) ) {
                    long nextHash = this.hasher.hash(next);

                    // checking the hash is faster than checking the file's existence - but may backfire
                    if (this.counts.containsKey(nextHash)) {
                        queue.add(next);
                        int count = this.counts.get(nextHash);
                        Node nextNode = NodeFactory.createURI(root + String.join("+", next) + this.extension);
                        Node remainingNode = NodeFactory.createLiteralByValue(count, TypeMapper.getInstance().getTypeByValue(count));

                        Node relationNode = NodeFactory.createBlankNode(String.join("+", next));
                        out.triple(Triple.create(thisNode, relationPredicate, relationNode));
                        out.triple(Triple.create(relationNode, typePredicate, relationObject));
                        out.triple(Triple.create(relationNode, nodePredicate, nextNode));
                        for (String token : next) {
                            Node tokenValue = NodeFactory.createLiteral(token);
                            out.triple(Triple.create(relationNode, valuePredicate, tokenValue));
                        }
                        out.triple(Triple.create(relationNode, treePathPredicate, pathNode));
                        out.triple(Triple.create(relationNode, shaclPatternPredicate, patternNode));
                        out.triple(Triple.create(relationNode, shaclFlagsPredicate, flagsNode));
                        out.triple(Triple.create(relationNode, remainingPredicate, remainingNode));
                    }
                }

                out.finish();
            }
        }

        System.out.println("Fullest page: " + mostWrittenPrefix + " @ " + mostWrittenCount);
    }
}

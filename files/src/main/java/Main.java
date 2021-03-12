package main.java;

import com.google.gson.Gson;
import org.apache.jena.graph.Node;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.riot.RDFParser;

import java.io.IOException;
import java.net.URI;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static java.lang.System.exit;

public class Main {
    public static void deleteDirectoryRecursive(Path path) throws IOException {
        // delete a directory and all its contents
        if (Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS)) {
            try (DirectoryStream<Path> entries = Files.newDirectoryStream(path)) {
                for (Path entry : entries) {
                    deleteDirectoryRecursive(entry);
                }
            }
        }
        Files.deleteIfExists(path);
    }

    public static void handleTask(
            URI domain,         // root URI used to identify all the fragments
            String outDir,      // local path to write the data files to
            ConfigTask task,    // description of the source data, and what to do with it
            int maxFileHandles,  // how many file handles can we have open while working
            String extension
    ) throws IOException {
        System.out.println("Parsing " + task.input);

        // prepare the output directory
        Path inputFileName = Path.of(task.input);
        Path outDirPath = Path.of(outDir, task.name);
        deleteDirectoryRecursive(outDirPath);
        Files.createDirectories(outDirPath);

        // convert the given properties to Property objects
        List<Node> properties = new ArrayList<>();
        for (String property : task.properties) {
            Property property1 = ResourceFactory.createProperty(property);
            Node asNode = property1.asNode();
            properties.add(asNode);
        }

        // send all data through a FragmentSink
        // which will pipe the triples to multiple fragment files
        Hasher hasher = new Hasher();
        FragmentSink fragmenter = new FragmentSink(properties, maxFileHandles, outDirPath, hasher, extension);
        RDFParser.source(inputFileName).parse(fragmenter);

        // we now know which fragments actually exist in the dataset
        // so now is the time to create links between them
        System.out.println("Finalizing " + task.input);
        HypermediaControls controls = new HypermediaControls(
                properties,
                fragmenter.getCounts(),
                fragmenter.getWritten(),
                hasher,
                outDirPath,
                fragmenter.getCharSet(),
                extension
        );
        controls.addHypermedia(domain.toASCIIString() + "/" + task.name + "/");
    }

    public static void main(String[] args) {
        Gson gson = new Gson();
        Path fileName = Path.of("config.json");
        try {
            String blob = Files.readString(fileName);
            Config config = gson.fromJson((blob), Config.class);

            for (ConfigTask task : config.tasks) {
                // process each file, one by one
                // this could be parallelized, but we'd just run into IO limitations
                String extension = task.extension == null ? ".nt" : task.extension;
                handleTask(URI.create(config.domain), config.outDir, task, config.maxFileHandles, extension);
            }
        } catch (IOException e) {
            e.printStackTrace(System.out);
            exit(1);
        }
    }
}

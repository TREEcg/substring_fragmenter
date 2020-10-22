package main.java;

import com.google.gson.Gson;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Main {
    public static void deleteDirectoryRecursion(Path path) throws IOException {
        if (Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS)) {
            try (DirectoryStream<Path> entries = Files.newDirectoryStream(path)) {
                for (Path entry : entries) {
                    deleteDirectoryRecursion(entry);
                }
            }
        }
        Files.deleteIfExists(path);
    }

    public static void handleTask(URI domain, String outDir, ConfigTask task, int maxFileHandles) throws IOException {
        System.out.println("Parsing " + task.input);
        Path inputFileName = Path.of(task.input);
        Path outDirPath = Path.of(outDir, task.name);
        deleteDirectoryRecursion(outDirPath);
        Files.createDirectories(outDirPath);
        ArrayList<Property> properties = Stream.of(task.properties)
                .map(ResourceFactory::createProperty)
                .collect(Collectors.toCollection(ArrayList::new));

        FragmentSink fragmenter = new FragmentSink(properties, maxFileHandles, outDirPath);
        RDFParser.source(inputFileName).parse(fragmenter);

        System.out.println("Finalizing " + task.input);
        fragmenter.addHypermedia(domain.resolve("./" + task.name + "/"));
    }

    public static void main(String[] args) {

        Gson gson = new Gson();
        Path fileName = Path.of("config.json");
        try {
            String blob = Files.readString(fileName);
            Config config = gson.fromJson((blob), Config.class);

            for (ConfigTask task : config.tasks) {
                handleTask(URI.create(config.domain), config.outDir, task, config.maxFileHandles);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

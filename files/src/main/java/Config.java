package main.java;

// simple object that Gson uses to deserialize the config.json file
public class Config {
    public ConfigTask[] tasks;
    public int maxFileHandles;
    public String outDir;
    public String domain;
}

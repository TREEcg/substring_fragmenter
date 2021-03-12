package main.java;

import javax.annotation.Nullable;

// simple object that Gson uses to deserialize the config.json file
public class ConfigTask {
    public String input;
    public String[] properties;
    public String name;

    @Nullable
    public String extension;
}

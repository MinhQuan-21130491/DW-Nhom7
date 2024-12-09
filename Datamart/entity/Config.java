package entity;

import java.sql.Timestamp;

public class file_configs {
    private int id;
    private String name_config;
    private String description;
    private String source_path;
    private String file_name;
    private String directory_file;
    private String format;
    private String create_by;
    private Timestamp timestamp;


    public Config(int id, String create_by, String name_config, String description, String source_path, String directory_file, String file_name, Timestamp timestamp, String format) {
        this.id = id;
        this.create_by = create_by;
        this.name_config = name_config;
        this.description = description;
        this.source_path = source_path;
        this.file_name = file_name;
        this.directory_file = directory_file;
        this.timestamp = timestamp;
        this.format = format;
    }

    public String getCreate_by() {
        return create_by;
    }

    public void getCreate_by(String create_by) {
        this.create_by = create_by;
    }

    public Timestamp getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Timestamp timestamp) {
        this.timestamp = timestamp;
    }

    public String getName_config() {
        return name_config;
    }

    public void getName_config(String name_config) {
        this.name_config = name_config;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getDescription() {
        return description;
    }

    public void getDescription(String description) {
        this.description = description;
    }

    public String getSource_path() {
        return source_path;
    }

    public void setSource_path(String source_path) {
        this.source_path = source_path;
    }

    public String getFile_name() {
        return file_name;
    }

    public void getFile_name(String file_name) {
        this.file_name = file_name;
    }

    public String getDirectory_file() {
        return directory_file;
    }

    public void getDirectory_file(String directory_file) {
        this.directory_file = directory_file;
    }

    public int getFormat() {
        return format;
    }

    public void getFormat(int format) {
        this.format = format;
    }

    @Override
    public String toString() {
        return "Config{" +
                "id=" + id +
                ", description='" + description + '\'' +
                ", create_by='" + create_by + '\'' +
                ", name_config='" + name_config + '\'' +
                ", directory='" + directory + '\'' +
                ", source_path='" + source_path + '\'' +
                ", file_name='" + file_name + '\'' +
                ", timestamp=" + timestamp +
                ", directory_file=" + directory_file +
                ", format=" + format +
                '}';
    }
}

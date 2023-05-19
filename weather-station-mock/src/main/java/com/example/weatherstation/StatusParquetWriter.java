package com.example.weatherstation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.hadoop.ParquetFileWriter.Mode;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupWriter;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class StatusParquetWriter extends ParquetWriter<Group> {

  public static void main(String[] args) throws IOException {
    Configuration conf = new Configuration();
    Path outputPath = new Path("output.parquet");

    // Define the schema for the Parquet file
    String schemaString = "message example {\n" +
        "  required int32 id;\n" +
        "  required binary name;\n" +
        "}";

    MessageType schema = MessageTypeParser.parseMessageType(schemaString);

    schema.getColumns().forEach(System.out::println);
    // Create a SimpleGroupFactory to create Group objects
    SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);

    try (// Create a ParquetWriter
        ParquetWriter<Group> writer = new StatusParquetWriter.Builder(outputPath)
            .withWriteMode(Mode.OVERWRITE)
            .withCompressionCodec(CompressionCodecName.SNAPPY)
            .withType(schema)
            .build()) {
      // Create a Group object and write it to the Parquet file
      Group group = groupFactory.newGroup()
          .append("id", 1)
          .append("name", "John Doe");
      writer.write(group);

      // Close the ParquetWriter
      writer.close();
    }
  }

  public static Builder builder(Path file) {
    return new Builder(file);
  }

  public static Builder builder(OutputFile file) {
    return new Builder(file);
  }

  StatusParquetWriter(Path file, WriteSupport<Group> writeSupport,
      CompressionCodecName compressionCodecName,
      int blockSize, int pageSize, boolean enableDictionary,
      boolean enableValidation,
      ParquetProperties.WriterVersion writerVersion,
      Configuration conf)
      throws IOException {
    super(file, writeSupport, compressionCodecName, blockSize, pageSize,
        pageSize, enableDictionary, enableValidation, writerVersion, conf);
  }

  public static class Builder extends ParquetWriter.Builder<Group, Builder> {
    private MessageType type = null;
    private Map<String, String> extraMetaData = new HashMap<String, String>();

    private Builder(Path file) {
      super(file);
    }

    private Builder(OutputFile file) {
      super(file);
    }

    public Builder withType(MessageType type) {
      this.type = type;
      return this;
    }

    @Override
    protected Builder self() {
      return this;
    }

    @Override
    protected WriteSupport<Group> getWriteSupport(Configuration conf) {
      GroupWriteSupport writerSupport = new GroupWriteSupport();
      writerSupport.setSchema(type, conf);
      return writerSupport;
    }

  }

  // crate a fucntion that dconverts a file to a string
  // static String getFileContent(File file) throws IOException {
  // byte[] fileBytes = Files.readAllBytes(Paths.get(file.getAbsolutePath()));

  // // Convert the byte array to a string using UTF-8 encoding
  // String fileContent = new String(fileBytes, StandardCharsets.UTF_8);

  // return fileContent;
  // }
}

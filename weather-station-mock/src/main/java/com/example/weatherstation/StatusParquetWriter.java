package com.example.weatherstation;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileWriter.Mode;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import com.example.weatherstation.models.Status;

public class StatusParquetWriter {

  int BATCH_SIZE = 10000;

  File outputDir;
  MessageType schema;
  /*
   * Map from station id to list of status records for that station
   */
  HashMap<String, List<Status>> statusRecords;

  StatusParquetWriter(File outputDir, File schemaFile) throws IOException {
    this.outputDir = outputDir;
    this.schema = MessageTypeParser.parseMessageType(
        getFileContent(schemaFile));
    statusRecords = new HashMap<>();
  }

  StatusParquetWriter(File outputDir, File schemaFile, int batchSize) throws IOException {
    this.BATCH_SIZE = batchSize;
    this.outputDir = outputDir;
    this.schema = MessageTypeParser.parseMessageType(
        getFileContent(schemaFile));
    statusRecords = new HashMap<>();
  }

  public void write(Status status) throws IOException {
    String stationId = status.getStationId().toString();
    if (!this.statusRecords.containsKey(stationId)) {
      this.statusRecords.put(stationId, new ArrayList<>());
    }
    this.statusRecords.get(stationId).add(status);

    if (this.statusRecords.get(stationId).size() >= BATCH_SIZE) {
      StatusParquetWriterHelper.writeToParquet(statusRecords.get(stationId), schema,
          new Path(outputDir.getAbsolutePath() + File.separator + UUID.randomUUID() + "." + stationId + ".parquet"));
      this.statusRecords.get(stationId).clear();
    }

  }

  static String getFileContent(File file) throws IOException {
    byte[] fileBytes = Files.readAllBytes(Paths.get(file.getAbsolutePath()));

    // Convert the byte array to a string using UTF-8 encoding
    String fileContent = new String(fileBytes, StandardCharsets.UTF_8);

    return fileContent;
  }
}

class StatusParquetWriterHelper extends ParquetWriter<Group> {

  public static void writeToParquet(List<Status> statusRecord, MessageType schema, Path outputPath) throws IOException {

    SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);

    try (
        ParquetWriter<Group> writer = new StatusParquetWriterHelper.Builder(outputPath)
            .withWriteMode(Mode.CREATE)
            .withCompressionCodec(CompressionCodecName.SNAPPY)
            .withType(schema)
            .build()) {

      statusRecord.stream().map(status -> {
        return groupFactory.newGroup()
            .append("stationId", status.getStationId())
            .append("sNo", status.getsNo())
            .append("batteryStatus", status.getBatteryStatus())
            .append("statusTimestamp", status.getStatusTimestamp())
            .append("weatherHumidity", status.getWeather().getHumidity())
            .append("weatherTemperature", status.getWeather().getTemperature())
            .append("weatherWindSpeed", status.getWeather().getWindSpeed());
      }).forEach(t -> {
        try {
          writer.write(t);
        } catch (IOException e) {
          System.out.println("Error writing status to parquet file" + t.toString());
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      });

    }
  }

  public static Builder builder(Path file) {
    return new Builder(file);
  }

  public static Builder builder(OutputFile file) {
    return new Builder(file);
  }

  StatusParquetWriterHelper(Path file, WriteSupport<Group> writeSupport,
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

}

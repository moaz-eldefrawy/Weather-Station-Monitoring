package com.example.weatherstation;

// import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
// import org.elasticsearch.hadoop.mr.EsInputFormat;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;

public class ParquetToElasticSearch {

  public static void main(String[] args)
      throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
    // Configuration conf = new Configuration();
    // conf.set(ConfigurationOptions.ES_NODES, ConfigurationOptions.ES_NODES_DEFAULT);
    // conf.set(ConfigurationOptions.ES_PORT, "9200");

    // Job job = Job.getInstance(conf);
    // job.setInputFormatClass(EsInputFormat.class);

    // // Configure the Parquet input format
    // job.getConfiguration().set("parquet.avro.read.schema",
    //     "/Users/moazel-defrawy/Desktop/4th year/data2/project/weather-station-mock/parquet.schema");

    // // Set the Elasticsearch output configuration
    // job.getConfiguration().set("es.resource", "your_index_name/type");
    // job.getConfiguration().set("es.output.json", "true");

 
    // // Execute the job
    // job.waitForCompletion(true);
  }
}
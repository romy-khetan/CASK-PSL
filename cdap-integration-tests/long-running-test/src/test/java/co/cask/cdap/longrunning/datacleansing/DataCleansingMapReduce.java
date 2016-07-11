/*
 * Copyright © 2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.longrunning.datacleansing;

import co.cask.cdap.api.ProgramLifecycle;
import co.cask.cdap.api.Resources;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.DynamicPartitioner;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.lib.PartitionKey;
import co.cask.cdap.api.dataset.lib.PartitionedFileSetArguments;
import co.cask.cdap.api.dataset.lib.partitioned.KVTableStatePersistor;
import co.cask.cdap.api.dataset.lib.partitioned.PartitionBatchInput;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.api.mapreduce.MapReduceTaskContext;
import co.cask.cdap.examples.datacleansing.SimpleSchemaMatcher;
import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonParser;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Modified {@link co.cask.cdap.examples.datacleansing.DataCleansingMapReduce} to write to readless table
 *
 * A simple MapReduce that reads records from the rawRecords PartitionedFileSet and writes all records
 * that match a particular {@link Schema} to the cleanRecords PartitionedFileSet. It also keeps track of its state of
 * which partitions it has processed, so that it only processes new partitions of data each time it runs.
 */
public class DataCleansingMapReduce extends AbstractMapReduce {
  protected static final String NAME = "DataCleansingMapReduce";
  protected static final String OUTPUT_PARTITION_KEY = "output.partition.key";
  protected static final String SCHEMA_KEY = "schema.key";

  private PartitionBatchInput.BatchPartitionCommitter partitionCommitter;

  @Override
  public void configure() {
    setName(NAME);
    setMapperResources(new Resources(1024));
    setReducerResources(new Resources(1024));
  }

  @Override
  public void beforeSubmit(MapReduceContext context) throws Exception {
    partitionCommitter =
      PartitionBatchInput.setInput(context, DataCleansingApp.RAW_RECORDS,
                                   new KVTableStatePersistor(DataCleansingApp.CONSUMING_STATE, "state.key"));

    // Each run writes its output to a partition for the league
    Long timeKey = Long.valueOf(context.getRuntimeArguments().get(OUTPUT_PARTITION_KEY));
    PartitionKey outputKey = PartitionKey.builder().addLongField("time", timeKey).build();

    Map<String, String> metadataToAssign = ImmutableMap.of("source.program", "DataCleansingMapReduce");

    // set up two outputs - one for invalid records and one for valid records
    Map<String, String> invalidRecordsArgs = new HashMap<>();
    PartitionedFileSetArguments.setOutputPartitionKey(invalidRecordsArgs, outputKey);
    PartitionedFileSetArguments.setOutputPartitionMetadata(invalidRecordsArgs, metadataToAssign);
    context.addOutput(DataCleansingApp.INVALID_RECORDS, invalidRecordsArgs);

    Map<String, String> cleanRecordsArgs = new HashMap<>();
    PartitionedFileSetArguments.setDynamicPartitioner(cleanRecordsArgs, TimeAndZipPartitioner.class);
    PartitionedFileSetArguments.setOutputPartitionMetadata(cleanRecordsArgs, metadataToAssign);
    context.addOutput(DataCleansingApp.CLEAN_RECORDS, cleanRecordsArgs);

    Job job = context.getHadoopJob();
    job.setMapperClass(SchemaMatchingFilter.class);
    job.setNumReduceTasks(0);

    // simply propagate the schema (if any) to be used by the mapper
    String schemaJson = context.getRuntimeArguments().get(SCHEMA_KEY);
    if (schemaJson != null) {
      job.getConfiguration().set(SCHEMA_KEY, schemaJson);
    }
  }

  @Override
  public void onFinish(boolean succeeded, MapReduceContext context) throws Exception {
    partitionCommitter.onFinish(succeeded);
  }

  /**
   * Partitions the records based upon a runtime argument (time) and a field extracted from the text being written (zip)
   */
  public static final class TimeAndZipPartitioner extends DynamicPartitioner<NullWritable, Text> {

    private Long time;
    private JsonParser jsonParser;

    @Override
    public void initialize(MapReduceTaskContext<NullWritable, Text> mapReduceTaskContext) {
      this.time = Long.valueOf(mapReduceTaskContext.getRuntimeArguments().get(OUTPUT_PARTITION_KEY));
      this.jsonParser = new JsonParser();
    }

    @Override
    public PartitionKey getPartitionKey(NullWritable key, Text value) {
      int zip = jsonParser.parse(value.toString()).getAsJsonObject().get("zip").getAsInt();
      return PartitionKey.builder().addLongField("time", time).addIntField("zip", zip).build();
    }
  }

  /**
   * A Mapper which skips text that doesn't match a given schema.
   */
  public static class SchemaMatchingFilter extends Mapper<LongWritable, Text, NullWritable, Text>
    implements ProgramLifecycle<MapReduceTaskContext<NullWritable, Text>> {
    public static final Schema DEFAULT_SCHEMA = Schema.recordOf("person",
                                                                Schema.Field.of("pid", Schema.of(Schema.Type.LONG)),
                                                                Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
                                                                Schema.Field.of("dob", Schema.of(Schema.Type.STRING)),
                                                                Schema.Field.of("zip", Schema.of(Schema.Type.INT)));

    private SimpleSchemaMatcher schemaMatcher;
    @UseDataSet(DataCleansingApp.TOTAL_RECORDS_TABLE)
    private KeyValueTable totalRecords;
    private MapReduceTaskContext<NullWritable, Text> mapReduceTaskContext;

    @Override
    public void initialize(MapReduceTaskContext<NullWritable, Text> context) throws Exception {
      this.mapReduceTaskContext = context;
    }

    @Override
    public void destroy() {
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      // deploy the schema to be used by the mapper
      String schemaJson = context.getConfiguration().get(SCHEMA_KEY);
      if (schemaJson == null) {
        schemaMatcher = new SimpleSchemaMatcher(DEFAULT_SCHEMA);
      } else {
        schemaMatcher = new SimpleSchemaMatcher(Schema.parseJson(schemaJson));
      }
    }

    public void map(LongWritable key, Text data, MapReduceTaskContext<NullWritable, Text> context)
      throws IOException, InterruptedException {
      if (!schemaMatcher.matches(data.toString())) {
        context.write(DataCleansingApp.INVALID_RECORDS, NullWritable.get(), data);
        totalRecords.increment(DataCleansingApp.INVALID_RECORD_KEY, 1);
      } else {
        context.write(DataCleansingApp.CLEAN_RECORDS, NullWritable.get(), data);
        totalRecords.increment(DataCleansingApp.CLEAN_RECORD_KEY, 1);
      }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      map(key, value, this.mapReduceTaskContext);
    }
  }
}

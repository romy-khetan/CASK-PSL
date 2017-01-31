/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.hydrator.plugin.batch.aggreagtor.aggregator;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchAggregator;
import co.cask.cdap.etl.api.batch.BatchAggregatorContext;
import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.collections.buffer.PriorityBuffer;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import javax.annotation.Nullable;

/**
 * Sampling plugin to sample random data from large dataset flowing through the plugin.
 */
@Plugin(type = BatchAggregator.PLUGIN_TYPE)
@Name("Sampling")
@Description("Sampling a large dataset flowing through this plugin to pull random records.")
public class Sampling extends BatchAggregator<String, StructuredRecord, StructuredRecord> {
    private enum TYPE {
        SYSTEMATIC, RESERVOIR
    }

    private SamplingConfig config;

    public Sampling(SamplingConfig config) {
        this.config = config;
    }

    @Override
    public void prepareRun(BatchAggregatorContext context) throws Exception {
        context.setNumPartitions(1);
    }

    @Override
    public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
        config.validate();
        pipelineConfigurer.getStageConfigurer()
                .setOutputSchema(pipelineConfigurer.getStageConfigurer().getInputSchema());
    }

    @Override
    public void groupBy(StructuredRecord record, Emitter<String> emitter) throws Exception {
        emitter.emit("sample");
    }

    @Override
    public void aggregate(String groupKey, Iterator<StructuredRecord> iterator,
                          Emitter<StructuredRecord> emitter) throws Exception {
        int finalSampleSize = 0;
        if (config.sampleSize != null) {
            finalSampleSize = config.sampleSize;
        }
        if (config.samplePercentage != null) {
            finalSampleSize = Math.round((config.samplePercentage / 100) * config.totalRecords);
        }

        switch (TYPE.valueOf(config.samplingType.toUpperCase())) {
            case SYSTEMATIC:
                if (config.overSamplingPercentage != null) {
                    finalSampleSize = Math.round(finalSampleSize +
                            (finalSampleSize * (config.overSamplingPercentage / 100)));
                }

                int sampleIndex = Math.round(config.totalRecords / finalSampleSize);
                Float random = new Float(0);
                if (config.random != null) {
                    random = config.random;
                } else {
                    random = new Random().nextFloat();
                }
                int firstSampleIndex = Math.round(sampleIndex * random);
                List<StructuredRecord> records = IteratorUtils.toList(iterator);
                int counter = 0;
                emitter.emit(records.get(firstSampleIndex));
                counter++;

                while (counter < finalSampleSize) {
                    int index = firstSampleIndex + (counter * sampleIndex);
                    emitter.emit(records.get(index - 1));
                    counter++;
                }
                break;

            case RESERVOIR:
                PriorityBuffer sampleData = new PriorityBuffer(true, new Comparator<StructuredRecord>() {
                    @Override
                    public int compare(StructuredRecord o1, StructuredRecord o2) {
                        if ((float) o1.get("random") < (float) o2.get("random")) {
                            return 1;
                        } else if ((float) o1.get("random") > (float) o2.get("random")) {
                            return -1;
                        } else {
                            return 0;
                        }
                    }
                });

                int count = 0;
                Random randomValue = new Random();
                List<StructuredRecord> recordArray = IteratorUtils.toList(iterator);
                Schema inputSchema = recordArray.get(0).getSchema();
                Schema schemaWithRandomField = createSchemaWithRandomField(inputSchema);
                while (count < finalSampleSize) {
                    StructuredRecord record = recordArray.get(0);
                    sampleData.add(getSampledRecord(record, randomValue.nextFloat(), schemaWithRandomField));
                    count++;
                }

                while (count < recordArray.size()) {
                    StructuredRecord structuredRecord = (StructuredRecord) sampleData.get();
                    Float randomFloat = randomValue.nextFloat();
                    if ((float) structuredRecord.get("random") < randomFloat) {
                        sampleData.remove();
                        StructuredRecord record = recordArray.get(count);
                        sampleData.add(getSampledRecord(record, randomFloat, structuredRecord.getSchema()));
                    }
                    count++;
                }

                Iterator<StructuredRecord> sampleDataIterator = sampleData.iterator();
                while (sampleDataIterator.hasNext()) {
                    StructuredRecord sampledRecord = sampleDataIterator.next();
                    StructuredRecord.Builder builder = StructuredRecord.builder(inputSchema);
                    for (Schema.Field field : sampledRecord.getSchema().getFields()) {
                        if (!field.getName().equalsIgnoreCase("random")) {
                            builder.set(field.getName(), sampledRecord.get(field.getName()));
                        }
                    }
                    emitter.emit(builder.build());
                }
                break;
        }
    }

    public StructuredRecord getSampledRecord(StructuredRecord record, Float random, Schema schema) {
        StructuredRecord.Builder builder = StructuredRecord.builder(schema);
        for (Schema.Field field : record.getSchema().getFields()) {
            builder.set(field.getName(), record.get(field.getName()));
        }
        builder.set("random", random);
        return builder.build();
    }

    /**
     * Builds the schema for Reservoir sampling algorithm. Adding field for random value.
     */
    private Schema createSchemaWithRandomField(Schema inputSchema) {
        List<Schema.Field> fields = new ArrayList<>();
        fields.add(Schema.Field.of("random", Schema.of(Schema.Type.FLOAT)));
        for (Schema.Field field : inputSchema.getFields()) {
            fields.add(field);
        }
        return Schema.recordOf("schema", fields);
    }

    /**
     * Config for Sampling Plugin.
     */
    public static class SamplingConfig extends PluginConfig {

        @Nullable
        @Description("The number of records that needs to be sampled from the input records.")
        private Integer sampleSize;

        @Nullable
        @Description("The percenatage of records that needs to be sampled from the input records.")
        private Float samplePercentage;

        @Description("Type of the Sampling algorithm that needs to be used to sample the data.")
        private String samplingType;

        @Nullable
        @Description("The percenatage of additional records that needs to be included in addition to the input " +
                "sample size to account for oversampling to be used in Systematic Sampling.")
        private Float overSamplingPercentage;

        @Nullable
        @Description("Random float value between 0 and 1 to be used in Systematic Sampling.")
        private Float random;

        @Nullable
        @Description("Total number od input records.")
        private Integer totalRecords;

        public SamplingConfig() {
            this.random = new Random().nextFloat();
        }

        public SamplingConfig(@Nullable Integer sampleSize, @Nullable Float samplePercentage,
                              @Nullable Float overSamplingPercentage, @Nullable Float random,
                              String samplingType, @Nullable Integer totalRecords) {
            this.sampleSize = sampleSize;
            this.samplePercentage = samplePercentage;
            this.overSamplingPercentage = overSamplingPercentage;
            this.random = random;
            this.samplingType = samplingType;
            this.totalRecords = totalRecords;
        }

        public void validate() {
            if (sampleSize == null && samplePercentage == null) {
                throw new IllegalArgumentException("Please provide Sample size or Sample Percentage values.");
            }

            if (samplingType.equalsIgnoreCase(TYPE.SYSTEMATIC.toString()) && totalRecords == null) {
                throw new IllegalArgumentException("Please provide value for 'Total Records' when selecting sampling " +
                        "type as 'Systematic'.");
            }
        }
    }
}

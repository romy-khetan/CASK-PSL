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

package co.cask.hydrator.plugin.batch.aggreagtor;

import co.cask.cdap.api.artifact.ArtifactVersion;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.datapipeline.DataPipelineApp;
import co.cask.cdap.datapipeline.SmartWorkflow;
import co.cask.cdap.etl.api.batch.BatchAggregator;
import co.cask.cdap.etl.mock.batch.MockSink;
import co.cask.cdap.etl.mock.batch.MockSource;
import co.cask.cdap.etl.mock.test.HydratorTestBase;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.artifact.ArtifactRange;
import co.cask.cdap.proto.artifact.ArtifactSummary;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.TestConfiguration;
import co.cask.cdap.test.WorkflowManager;
import co.cask.hydrator.plugin.batch.aggreagtor.aggregator.Sampling;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;


import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Test for Sampling plugin
 */
public class SamplingTest extends HydratorTestBase {
    @ClassRule
    public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

    protected static final ArtifactId BATCH_ARTIFACT_ID = NamespaceId.DEFAULT.artifact("data-pipeline", "4.0.0");
    protected static final ArtifactSummary BATCH_ARTIFACT = new ArtifactSummary("data-pipeline", "4.0.0");

    protected static final Schema SOURCE_SCHEMA =
            Schema.recordOf("sourceRecord",
                    Schema.Field.of(SamplingTest.ID, Schema.of(Schema.Type.STRING)),
                    Schema.Field.of(SamplingTest.NAME, Schema.of(Schema.Type.STRING)),
                    Schema.Field.of(SamplingTest.SALARY, Schema.of(Schema.Type.STRING)),
                    Schema.Field.of(SamplingTest.DESIGNATIONID, Schema.nullableOf(Schema.of(Schema.Type.STRING))));

    private static final String ID = "id";
    private static final String NAME = "name";
    private static final String SALARY = "salary";
    private static final String DESIGNATIONID = "designationid";

    @BeforeClass
    public static void setupTestClass() throws Exception {

        setupBatchArtifacts(BATCH_ARTIFACT_ID, DataPipelineApp.class);
        Set<ArtifactRange> parents = new HashSet<>();
        parents.add(new ArtifactRange(NamespaceId.DEFAULT, BATCH_ARTIFACT_ID.getArtifact(),
                new ArtifactVersion(BATCH_ARTIFACT.getVersion()), true,
                new ArtifactVersion(BATCH_ARTIFACT.getVersion()), true));
        addPluginArtifact(NamespaceId.DEFAULT.artifact("sampling-aggregator-plugin", "1.6.0"), parents,
                Sampling.class);
    }

    @Test
    public void testSystematicSampling() throws Exception {
        String inputTable = "input_table";
        ETLStage source = new ETLStage("source", MockSource.getPlugin(inputTable));

        Map<String, String> sourceproperties = new ImmutableMap.Builder<String, String>()
                .put("sampleSize", "2")
                .put("random", "0.2")
                .put("totalRecords", "4")
                .put("samplingType", "Systematic")
                .build();

        ETLStage transform = new ETLStage("aggregate",
                new ETLPlugin("Sampling", BatchAggregator.PLUGIN_TYPE, sourceproperties, null));

        String sinkTable = "output_table";
        ETLStage sink = new ETLStage("sink", MockSink.getPlugin(sinkTable));

        ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
                .addStage(source)
                .addStage(transform)
                .addStage(sink)
                .addConnection(source.getName(), transform.getName())
                .addConnection(transform.getName(), sink.getName())
                .build();

        AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(BATCH_ARTIFACT, etlConfig);
        ApplicationId appId = NamespaceId.DEFAULT.app("SystematicSamplingTest");
        ApplicationManager appManager = deployApplication(appId, appRequest);

        DataSetManager<Table> inputManager = getDataset(inputTable);
        List<StructuredRecord> input = ImmutableList.of(
                StructuredRecord.builder(SOURCE_SCHEMA).set(ID, "100").set(NAME, "John").set(SALARY, "1000")
                        .set(DESIGNATIONID, null).build(),
                StructuredRecord.builder(SOURCE_SCHEMA).set(ID, "101").set(NAME, "Kerry").set(SALARY, "1030")
                        .set(DESIGNATIONID, "2").build(),
                StructuredRecord.builder(SOURCE_SCHEMA).set(ID, "102").set(NAME, "Mathew").set(SALARY, "1230")
                        .set(DESIGNATIONID, "").build(),
                StructuredRecord.builder(SOURCE_SCHEMA).set(ID, "103").set(NAME, "Allie").set(SALARY, "2000")
                        .set(DESIGNATIONID, "4").build());
        MockSource.writeInput(inputManager, input);

        WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
        workflowManager.start();
        workflowManager.waitForFinish(5, TimeUnit.MINUTES);

        DataSetManager<Table> outputManager = getDataset(sinkTable);
        List<StructuredRecord> outputRecords = MockSink.readOutput(outputManager);
        Assert.assertEquals(2, outputRecords.size());
    }

    @Test
    public void testSystematicSamplingWithOverSampling() throws Exception {
        String inputTable = "input_table";
        ETLStage source = new ETLStage("source", MockSource.getPlugin(inputTable));

        Map<String, String> sourceproperties = new ImmutableMap.Builder<String, String>()
                .put("sampleSize", "2")
                .put("overSamplingPercentage", "30")
                .put("totalRecords", "4")
                .put("samplingType", "Systematic")
                .build();

        ETLStage transform = new ETLStage("aggregate",
                new ETLPlugin("Sampling", BatchAggregator.PLUGIN_TYPE, sourceproperties, null));

        String sinkTable = "output_table_with_oversampling";
        ETLStage sink = new ETLStage("sink", MockSink.getPlugin(sinkTable));

        ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
                .addStage(source)
                .addStage(transform)
                .addStage(sink)
                .addConnection(source.getName(), transform.getName())
                .addConnection(transform.getName(), sink.getName())
                .build();

        AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(BATCH_ARTIFACT, etlConfig);
        ApplicationId appId = NamespaceId.DEFAULT.app("SystematicSamplingWithOverSamplingTest");
        ApplicationManager appManager = deployApplication(appId, appRequest);

        DataSetManager<Table> inputManager = getDataset(inputTable);
        List<StructuredRecord> input = ImmutableList.of(
                StructuredRecord.builder(SOURCE_SCHEMA).set(ID, "100").set(NAME, "John").set(SALARY, "1000")
                        .set(DESIGNATIONID, null).build(),
                StructuredRecord.builder(SOURCE_SCHEMA).set(ID, "101").set(NAME, "Kerry").set(SALARY, "1030")
                        .set(DESIGNATIONID, "2").build(),
                StructuredRecord.builder(SOURCE_SCHEMA).set(ID, "102").set(NAME, "Mathew").set(SALARY, "1230")
                        .set(DESIGNATIONID, "").build(),
                StructuredRecord.builder(SOURCE_SCHEMA).set(ID, "103").set(NAME, "Allie").set(SALARY, "2000")
                        .set(DESIGNATIONID, "4").build());
        MockSource.writeInput(inputManager, input);

        WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
        workflowManager.start();
        workflowManager.waitForFinish(5, TimeUnit.MINUTES);

        DataSetManager<Table> outputManager = getDataset(sinkTable);
        List<StructuredRecord> outputRecords = MockSink.readOutput(outputManager);
        Assert.assertEquals(3, outputRecords.size());
    }

    @Test
    public void testReservoirSampling() throws Exception {
        String inputTable = "input_table";
        ETLStage source = new ETLStage("source", MockSource.getPlugin(inputTable));

        Map<String, String> sourceproperties = new ImmutableMap.Builder<String, String>()
                .put("sampleSize", "2")
                .put("samplingType", "Reservoir")
                .build();

        ETLStage transform = new ETLStage("aggregate",
                new ETLPlugin("Sampling", BatchAggregator.PLUGIN_TYPE, sourceproperties, null));

        String sinkTable = "output_table_reservoir";
        ETLStage sink = new ETLStage("sink", MockSink.getPlugin(sinkTable));

        ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
                .addStage(source)
                .addStage(transform)
                .addStage(sink)
                .addConnection(source.getName(), transform.getName())
                .addConnection(transform.getName(), sink.getName())
                .build();

        AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(BATCH_ARTIFACT, etlConfig);
        ApplicationId appId = NamespaceId.DEFAULT.app("ReservoirSamplingTest");
        ApplicationManager appManager = deployApplication(appId, appRequest);

        DataSetManager<Table> inputManager = getDataset(inputTable);
        List<StructuredRecord> input = ImmutableList.of(
                StructuredRecord.builder(SOURCE_SCHEMA).set(ID, "100").set(NAME, "John").set(SALARY, "1000")
                        .set(DESIGNATIONID, null).build(),
                StructuredRecord.builder(SOURCE_SCHEMA).set(ID, "101").set(NAME, "Kerry").set(SALARY, "1030")
                        .set(DESIGNATIONID, "2").build(),
                StructuredRecord.builder(SOURCE_SCHEMA).set(ID, "102").set(NAME, "Mathew").set(SALARY, "1230")
                        .set(DESIGNATIONID, "").build(),
                StructuredRecord.builder(SOURCE_SCHEMA).set(ID, "103").set(NAME, "Allie").set(SALARY, "2000")
                        .set(DESIGNATIONID, "4").build());
        MockSource.writeInput(inputManager, input);

        WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
        workflowManager.start();
        workflowManager.waitForFinish(5, TimeUnit.MINUTES);

        DataSetManager<Table> outputManager = getDataset(sinkTable);
        List<StructuredRecord> outputRecords = MockSink.readOutput(outputManager);
        Assert.assertEquals(2, outputRecords.size());
        Assert.assertEquals(outputRecords.get(0).getSchema(), SOURCE_SCHEMA);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSamplingWithoutSampleSizeOrPercentage() {
        Sampling.SamplingConfig config = new Sampling.SamplingConfig(null, null, Float.valueOf(10), null,
                "Systematic", 10);
        config.validate();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSystematicSamplingWithoutTotalRecords() {
        Sampling.SamplingConfig config = new Sampling.SamplingConfig(10, null, Float.valueOf(10), null,
                "Systematic", null);
        config.validate();
    }
}

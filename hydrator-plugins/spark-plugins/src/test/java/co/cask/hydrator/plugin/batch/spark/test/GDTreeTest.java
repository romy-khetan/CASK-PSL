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

package co.cask.hydrator.plugin.batch.spark.test;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.datapipeline.DataPipelineApp;
import co.cask.cdap.datapipeline.SmartWorkflow;
import co.cask.cdap.etl.api.batch.SparkSink;
import co.cask.cdap.etl.mock.batch.MockSource;
import co.cask.cdap.etl.mock.test.HydratorTestBase;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.artifact.ArtifactSummary;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.TestConfiguration;
import co.cask.cdap.test.WorkflowManager;
import co.cask.hydrator.plugin.spark.GDTreeTrainer;
import co.cask.hydrator.plugin.spark.TwitterStreamingSource;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class GDTreeTest extends HydratorTestBase {

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);
  @ClassRule
  public static TemporaryFolder temporaryFolder = new TemporaryFolder();

  protected static final ArtifactId DATAPIPELINE_ARTIFACT_ID = NamespaceId.DEFAULT.artifact("data-pipeline", "3.5.0");
  protected static final ArtifactSummary DATAPIPELINE_ARTIFACT = new ArtifactSummary("data-pipeline", "3.5.0");

  private final Schema schema =
    Schema.recordOf("flightData", Schema.Field.of("dofM", Schema.nullableOf(Schema.of(Schema.Type.INT))),
                    Schema.Field.of("dofW", Schema.nullableOf(Schema.of(Schema.Type.INT))),
                    Schema.Field.of("carrier", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
                    Schema.Field.of("tailNum", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                    Schema.Field.of("flightNum", Schema.nullableOf(Schema.of(Schema.Type.INT))),
                    Schema.Field.of("originId", Schema.nullableOf(Schema.of(Schema.Type.INT))),
                    Schema.Field.of("origin", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                    Schema.Field.of("destId", Schema.nullableOf(Schema.of(Schema.Type.INT))),
                    Schema.Field.of("dest", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                    Schema.Field.of("scheduleDepTime", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
                    Schema.Field.of("deptime", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
                    Schema.Field.of("depDelayMins", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
                    Schema.Field.of("scheduledArrTime", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
                    Schema.Field.of("arrTime", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
                    Schema.Field.of("arrDelay", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
                    Schema.Field.of("elapsedTime", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
                    Schema.Field.of("distance", Schema.nullableOf(Schema.of(Schema.Type.INT))));

  private static File sourceFolder;

  @BeforeClass
  public static void setupTest() throws Exception {
    // add the artifact for etl batch app
    setupBatchArtifacts(DATAPIPELINE_ARTIFACT_ID, DataPipelineApp.class);

    // add artifact for spark plugins
    addPluginArtifact(NamespaceId.DEFAULT.artifact("spark-plugins", "1.0.0"), DATAPIPELINE_ARTIFACT_ID,
                      GDTreeTrainer.class, TwitterStreamingSource.class);

    sourceFolder = temporaryFolder.newFolder("GDTree");
  }

  @AfterClass
  public static void tearDown() throws Exception {
    temporaryFolder.delete();
  }

  @Before
  public void copyFiles() throws Exception {
    URL testFileUrl = this.getClass().getResource("/trainData.csv");
    FileUtils.copyFile(new File(testFileUrl.getFile()), new File(sourceFolder, "/trainData.csv"));
  }

  @Test
  public void testSparkSinkAndCompute() throws Exception {
    // use the SparkSink(GDTreeTrainer) to train a model
    testSinglePhaseWithSparkSink();
  }

  private void testSinglePhaseWithSparkSink() throws Exception {
    /*
     * source --> sparksink
     */
    String inputTable = "flight-data";
    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      .put("fileSetName", "gd-tree-model")
      .put("path", "output")
      .put("featuresToInclude", "dofM,dofW,carrier,originId,destId,scheduleDepTime,scheduledArrTime,elapsedTime")
      .put("labelField", "delayed")
      .put("maxClass", "2")
      .put("maxDepth", "10")
      .put("maxIteration", "5")
      .build();

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source", MockSource.getPlugin(inputTable, getTrainerSchema(schema))))
      .addStage(new ETLStage("customsink", new ETLPlugin(GDTreeTrainer.PLUGIN_NAME, SparkSink.PLUGIN_TYPE,
                                                         properties, null)))
      .addConnection("source", "customsink")
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(DATAPIPELINE_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("SinglePhaseApp");
    ApplicationManager appManager = deployApplication(appId.toId(), appRequest);

    // send records from sample data to train the model
    List<StructuredRecord> messagesToWrite = new ArrayList<>();
    messagesToWrite.addAll(getInputData());

    // write records to source
    DataSetManager<Table> inputManager = getDataset(Id.Namespace.DEFAULT, inputTable);
    MockSource.writeInput(inputManager, messagesToWrite);

    // manually trigger the pipeline
    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForFinish(5, TimeUnit.MINUTES);
  }

  //Get data from file to be used for training the model.
  private List<StructuredRecord> getInputData() throws IOException {
    List<StructuredRecord> messagesToWrite = new ArrayList<>();
    File file = new File(sourceFolder.getAbsolutePath(), "/trainData.csv");
    BufferedReader bufferedInputStream = new BufferedReader(new FileReader(file));
    String line;
    while ((line = bufferedInputStream.readLine()) != null) {
      String[] flightData = line.split(",");
      Double depDelayMins = Double.parseDouble(flightData[11]);
      //For binary classification create delayed field containing values 1.0 and 0.0 depending on the delay time.
      double delayed = depDelayMins > 40 ? 1.0 : 0.0;
      messagesToWrite.add(new Flight(Integer.parseInt(flightData[0]), Integer.parseInt(flightData[1]),
                                     Double.parseDouble(flightData[2]), flightData[3], Integer.parseInt(flightData[4]),
                                     Integer.parseInt(flightData[5]), flightData[6], Integer.parseInt(flightData[7]),
                                     flightData[8], Integer.parseInt(flightData[9]), Double.parseDouble(flightData[10]),
                                     depDelayMins, Double.parseDouble(flightData[12]),
                                     Double.parseDouble(flightData[13]), Double.parseDouble(flightData[14]),
                                     Double.parseDouble(flightData[15]), Integer.parseInt(flightData[16]), delayed)
                            .toStructuredRecord());
    }
    return messagesToWrite;
  }

  private Schema getTrainerSchema(Schema schema) {
    List<Schema.Field> fields = new ArrayList<>(schema.getFields());
    fields.add(Schema.Field.of("delayed", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))));
    return Schema.recordOf(schema.getRecordName() + ".predicted", fields);
  }
}

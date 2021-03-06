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

package co.cask.cdap.app.etl.batch;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.app.etl.ETLTestBase;
import co.cask.cdap.client.QueryClient;
import co.cask.cdap.datapipeline.SmartWorkflow;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.batch.SparkCompute;
import co.cask.cdap.etl.api.batch.SparkSink;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.explore.client.ExploreExecutionResult;
import co.cask.cdap.proto.Id;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.StreamManager;
import co.cask.cdap.test.WorkflowManager;
import co.cask.cdap.test.suite.category.CDH51Incompatible;
import co.cask.cdap.test.suite.category.CDH52Incompatible;
import co.cask.cdap.test.suite.category.CDH53Incompatible;
import co.cask.cdap.test.suite.category.HDP20Incompatible;
import co.cask.cdap.test.suite.category.HDP21Incompatible;
import co.cask.cdap.test.suite.category.MapR5Incompatible;
import co.cask.hydrator.plugin.common.Properties;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@Category({
  // We don't support spark on these distros
  HDP20Incompatible.class,
  HDP21Incompatible.class,
  CDH51Incompatible.class,
  CDH52Incompatible.class,
  // this test is only compatible with spark v1.3.0 onwards and cdh5.3 uses spark v1.2.0
  CDH53Incompatible.class,
  // Currently, coopr doesn't provision MapR cluster with Spark
  MapR5Incompatible.class // MapR51 category is used for all MapR version
})

public class GDTreeTest extends ETLTestBase {

  private final Schema sourceSchema = Schema.recordOf("sourceRecord",
                                                      Schema.Field.of("dofM", Schema.of(Schema.Type.INT)),
                                                      Schema.Field.of("dofW", Schema.of(Schema.Type.INT)),
                                                      Schema.Field.of("carrier", Schema.of(Schema.Type.DOUBLE)),
                                                      Schema.Field.of("tailNum", Schema.of(Schema.Type.STRING)),
                                                      Schema.Field.of("flightNum", Schema.of(Schema.Type.INT)),
                                                      Schema.Field.of("originId", Schema.of(Schema.Type.INT)),
                                                      Schema.Field.of("origin", Schema.of(Schema.Type.STRING)),
                                                      Schema.Field.of("destId", Schema.of(Schema.Type.INT)),
                                                      Schema.Field.of("dest", Schema.of(Schema.Type.STRING)),
                                                      Schema.Field.of("scheduleDepTime", Schema.of(Schema.Type.DOUBLE)),
                                                      Schema.Field.of("deptime", Schema.of(Schema.Type.DOUBLE)),
                                                      Schema.Field.of("depDelayMins", Schema.of(Schema.Type.DOUBLE)),
                                                      Schema.Field.of("scheduledArrTime",
                                                                      Schema.of(Schema.Type.DOUBLE)),
                                                      Schema.Field.of("arrTime", Schema.of(Schema.Type.DOUBLE)),
                                                      Schema.Field.of("arrDelay", Schema.of(Schema.Type.DOUBLE)),
                                                      Schema.Field.of("elapsedTime", Schema.of(Schema.Type.DOUBLE)),
                                                      Schema.Field.of("distance", Schema.of(Schema.Type.INT)));

  @Test
  public void testSparkPlugins() throws Exception {
    // use the SparkSink to train a GD-Tree model
    testSparkSink();
    // use a SparkCompute to classify all records going through the pipeline, using the model build with the SparkSink
    testSparkCompute();
  }

  private void testSparkSink() throws Exception {
    /*
     * stream --> transform --> sparksink
     */
    String script = "function transform(input, emitter, context) {" +
      "  var output = input;" +
      "  output.delayed = input.depDelayMins > 40 ? 1.0 : 0.0;" +
      "  emitter.emit(output);" +
      "}";

    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      .put("fileSetName", "gd-tree-model")
      .put("path", "output")
      .put("featuresToInclude", "dofM,dofW,carrier,originId,destId,scheduleDepTime,scheduledArrTime,elapsedTime")
      .put("labelField", "delayed")
      .put("maxClass", "2")
      .put("maxDepth", "10")
      .put("maxIteration", "5")
      .build();
    ETLPlugin streamPlugin = new ETLPlugin("Stream", BatchSource.PLUGIN_TYPE,
                                           ImmutableMap.of(Properties.Stream.NAME, "trainingStream",
                                                           Properties.Stream.DURATION, "1d",
                                                           Properties.Stream.FORMAT, "csv",
                                                           Properties.Stream.SCHEMA, sourceSchema.toString()), null);
    ETLPlugin javaScriptPlugin = new ETLPlugin("JavaScript", Transform.PLUGIN_TYPE,
                                               ImmutableMap.of("script", script,
                                                               "schema", getSinkSchema(sourceSchema).toString()), null);

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source", streamPlugin))
      .addStage(new ETLStage("eventParser", javaScriptPlugin))
      .addStage(new ETLStage("customSink", new ETLPlugin("GDTreeTrainer", SparkSink.PLUGIN_TYPE, properties, null)))
      .addConnection("source", "eventParser")
      .addConnection("eventParser", "customSink")
      .build();

    ApplicationManager appManager = deployApplication(Id.Application.from(TEST_NAMESPACE, "FlightDelayTrainer"),
                                                      getBatchAppRequestV2(etlConfig));

    // write records to source
    StreamManager streamManager =
      getTestManager().getStreamManager(Id.Stream.from(TEST_NAMESPACE, "trainingStream"));

    List<String> messagesToWrite = ImmutableList.of(
      "15,3,3,N26549,6157,13296,MHT,13930,ORD,1636,1659,23,1825,1830,5,169,843",
      "19,7,3,N13992,6157,13296,MHT,13930,ORD,1636,1633,0,1825,1826,1,169,843",
      "21,2,3,N10575,6157,13296,MHT,13930,ORD,1636,1653,17,1825,1829,4,169,843",
      "29,3,3,N13968,6157,13296,MHT,13930,ORD,1636,1652,16,1825,1831,6,169,843",
      "14,2,3,N13970,6157,13296,MHT,13930,ORD,1636,1632,0,1825,1829,4,169,843",
      "22,3,3,N14977,6157,13296,MHT,13930,ORD,1636,1713,37,1825,1902,37,169,843",
      "8,3,3,N12924,6157,13296,MHT,13930,ORD,1636,1718,42,1825,1912,47,169,843",
      "26,7,3,N14993,6157,13296,MHT,13930,ORD,1636,1632,0,1825,1830,5,169,843",
      "3,5,3,N11548,6157,13930,ORD,13296,MHT,1310,1605,175,1621,1916,175,131,843",
      "3,5,3,N11548,6157,13296,MHT,13930,ORD,1652,1951,179,1845,2150,185,173,843",
      "12,7,3,N13968,6157,13296,MHT,13930,ORD,1636,1659,23,1825,1843,18,169,843",
      "15,3,3,N26549,6157,13296,MHT,13930,ORD,1636,1659,23,1825,1830,5,169,843",
      "19,7,3,N13992,6157,13296,MHT,13930,ORD,1636,1633,0,1825,1826,1,169,843",
      "21,2,3,N10575,6157,13296,MHT,13930,ORD,1636,1653,17,1825,1829,4,169,843",
      "29,3,3,N13968,6157,13296,MHT,13930,ORD,1636,1652,16,1825,1831,6,169,843",
      "14,2,3,N13970,6157,13296,MHT,13930,ORD,1636,1632,0,1825,1829,4,169,843",
      "22,3,3,N14977,6157,13296,MHT,13930,ORD,1636,1713,37,1825,1902,37,169,843",
      "8,3,3,N12924,6157,13296,MHT,13930,ORD,1636,1718,42,1825,1912,47,169,843",
      "26,7,3,N14993,6157,13296,MHT,13930,ORD,1636,1632,0,1825,1830,5,169,843",
      "3,5,3,N11548,6157,13930,ORD,13296,MHT,1310,1605,175,1621,1916,175,131,843",
      "26,7,3,N14993,6157,13930,ORD,13296,MHT,1256,1303,7,1606,1555,0,130,843",
      "5,7,3,N41104,6158,11292,DEN,11540,ELP,1930,1945,15,2118,2138,20,108,563",
      "3,5,3,N12145,6158,11292,DEN,11540,ELP,1930,2034,64,2118,2216,58,108,563");

    for(String message : messagesToWrite) {
      streamManager.send(message);
    }

    // manually trigger the pipeline
    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForFinish(10, TimeUnit.MINUTES);
  }

  private Schema getSinkSchema(Schema sourceSchema) {
    List<Schema.Field> fields = new ArrayList<>(sourceSchema.getFields());
    fields.add(Schema.Field.of("delayed", Schema.of(Schema.Type.DOUBLE)));
    return Schema.recordOf("sinkRecord", fields);
  }

  private void testSparkCompute() throws Exception {
    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      .put("fileSetName", "gd-tree-model")
      .put("path", "output")
      .put("featuresToInclude", "dofM,dofW,carrier,originId,destId,scheduleDepTime,scheduledArrTime,elapsedTime")
      .put("predictionField", "delayed")
      .build();

    ETLPlugin streamPlugin = new ETLPlugin("Stream", BatchSource.PLUGIN_TYPE,
                                           ImmutableMap.of(Properties.Stream.NAME, "testStream",
                                                           Properties.Stream.DURATION, "1d",
                                                           Properties.Stream.FORMAT, "csv",
                                                           Properties.Stream.SCHEMA, sourceSchema.toString()), null);

    ETLPlugin projecttionPlugin = new ETLPlugin("Projection", Transform.PLUGIN_TYPE,
                                                ImmutableMap.of("name", "headers",
                                                                "drop", "id,headers",
                                                                "schema", sourceSchema.toString()), null);

    ETLPlugin sinkPlugin = new ETLPlugin("TPFSAvro", BatchSink.PLUGIN_TYPE,
                                         ImmutableMap.of("name", "gdtreesink",
                                                        "schema", getSinkSchema(sourceSchema).toString()),
                                         null);

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source", streamPlugin))
      .addStage(new ETLStage("projectionTransform", projecttionPlugin))
      .addStage(new ETLStage("sparkCompute", new ETLPlugin("GDTreeClassifier", SparkCompute.PLUGIN_TYPE,
                                                           properties, null)))
      .addStage(new ETLStage("sink", sinkPlugin))
      .addConnection("source", "projectionTransform")
      .addConnection("projectionTransform", "sparkCompute")
      .addConnection("sparkCompute", "sink")
      .build();

    ApplicationManager appManager = deployApplication(Id.Application.from(TEST_NAMESPACE, "GDTreeClassifier"),
                                                      getBatchAppRequestV2(etlConfig));

    // write some some messages to be classified
    StreamManager streamManager =
      getTestManager().getStreamManager(Id.Stream.from(TEST_NAMESPACE, "testStream"));

    streamManager.send("25,6,2,N0EGMQ,3419,10397,ATL,12953,LGA,1150,1229,39,1359,1448,49,129,762");
    streamManager.send("29,3,1,N355AA,2407,12892,LAX,11298,DFW,1025,1023,0,1530,1523,0,185,1235");
    streamManager.send("2,4,4,N919DE,1908,13930,ORD,11433,DTW,1641,1902,141,1905,2117,132,84,235");
    streamManager.send("2,4,4,N933DN,1791,10397,ATL,15376,TUS,1855,2014,79,2108,2159,51,253,1541");

    // manually trigger the pipeline
    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForFinish(10, TimeUnit.MINUTES);
    Map<Double, Double> delayedPredictionMap = new HashMap<>();
    delayedPredictionMap.put(39.0, 0.0);
    delayedPredictionMap.put(0.0, 0.0);
    delayedPredictionMap.put(141.0, 1.0);
    delayedPredictionMap.put(79.0, 1.0);
    Assert.assertEquals(delayedPredictionMap, getDelayedPredictionMap());
  }

  private Map<Double, Double> getDelayedPredictionMap() throws ExecutionException, InterruptedException {
    QueryClient queryClient = new QueryClient(getClientConfig());
    ExploreExecutionResult exploreExecutionResult =
      queryClient.execute(TEST_NAMESPACE, "SELECT * FROM dataset_gdtreesink").get();

    Map<Double, Double> predictionMap = new HashMap<>();
    while (exploreExecutionResult.hasNext()) {
      List<Object> columns = exploreExecutionResult.next().getColumns();
      predictionMap.put((Double) columns.get(11), (Double) columns.get(17));
    }
    return predictionMap;
  }
}

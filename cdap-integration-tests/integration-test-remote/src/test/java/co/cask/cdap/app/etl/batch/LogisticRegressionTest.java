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

import co.cask.cdap.api.Resources;
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
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.StreamManager;
import co.cask.cdap.test.WorkflowManager;
import co.cask.cdap.test.suite.category.CDH51Incompatible;
import co.cask.cdap.test.suite.category.CDH52Incompatible;
import co.cask.cdap.test.suite.category.CDH53Incompatible;
import co.cask.cdap.test.suite.category.HDP20Incompatible;
import co.cask.cdap.test.suite.category.HDP21Incompatible;
import co.cask.cdap.test.suite.category.MapR5Incompatible;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Test for {@link SparkSink} plugin type, using LogisticRegressionTrainer.
 */
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
public class LogisticRegressionTest extends ETLTestBase {

  @Test
  public void testSparkPlugins() throws Exception {
    // use the SparkSink to train a logistic regression model
    testSparkSink();
    // use a SparkCompute to classify all records going through the pipeline, using the model build with the SparkSink
    testSparkCompute();
  }

  private void testSparkSink() throws Exception {
    /*
     * stream --> transform --> sparksink
     */
    String script = "function transform(input, emitter, context) {" +
      "  parts = input.body.split(',');" +
      "  text = parts[0].split(' ');" +
      "  input.isSpam = text[0] === 'spam' ? 1.0 : 0;" +
      "  text.shift();" +
      "  input.text = text.join(' ');" +
      "  input.boolField = parts[1];" +
      "  emitter.emit(input);" +
      "}";

    Map<String, String> sourceProp = ImmutableMap.of("name", "logisticRegressionTrainingStream",
                                                     "duration", "1d",
                                                     "format", "csv",
                                                     "schema", LogisticRegressionSpamMessage.SCHEMA.toString());

    Map<String, String> transformProp = ImmutableMap.of("script", script,
                                                        "schema", LogisticRegressionSpamMessage.SCHEMA.toString());

    String fieldsToClassify = LogisticRegressionSpamMessage.TEXT_FIELD + "," + LogisticRegressionSpamMessage.READ_FIELD;
    Map<String, String> sinkProp = ImmutableMap.of("fileSetName", "modelFileSet",
                                                   "path", "output",
                                                   "fieldsToClassify", fieldsToClassify,
                                                   "predictionField",
                                                   LogisticRegressionSpamMessage.SPAM_PREDICTION_FIELD,
                                                   "numClasses", "2");
    ETLPlugin sparkPlugin = new ETLPlugin("LogisticRegressionTrainer", SparkSink.PLUGIN_TYPE, sinkProp, null);

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source", new ETLPlugin("Stream", BatchSource.PLUGIN_TYPE, sourceProp, null)))
      .addStage(new ETLStage("eventParser", new ETLPlugin("JavaScript", Transform.PLUGIN_TYPE, transformProp, null)))
      .addStage(new ETLStage("customSink", sparkPlugin))
      .addConnection("source", "eventParser")
      .addConnection("eventParser", "customSink")
      .build();

    AppRequest<ETLBatchConfig> request = getBatchAppRequestV2(etlConfig);
    Id.Application appId = Id.Application.from(TEST_NAMESPACE, "LogisticRegressionSpamTrainer");
    ApplicationManager appManager = deployApplication(appId, request);

    // set up five spam messages and five non-spam messages to be used for classification
    List<String> trainingMessages = ImmutableList.of("spam buy our clothes,yes",
                                                     "spam sell your used books to us,yes",
                                                     "spam earn money for free,yes",
                                                     "spam this is definitely not spam,yes",
                                                     "spam you won the lottery,yes",
                                                     "ham how was your day,no",
                                                     "ham what are you up to,no",
                                                     "ham this is a genuine message,no",
                                                     "ham this is an even more genuine message,no",
                                                     "ham could you send me the report,no");

    Id.Stream stream = Id.Stream.from(Id.Namespace.DEFAULT, "logisticRegressionTrainingStream");

    // write records to source
    StreamManager streamManager = getTestManager().getStreamManager(stream);
    for (String spamMessage : trainingMessages) {
      streamManager.send(spamMessage);
    }

    // manually trigger the pipeline
    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForFinish(10, TimeUnit.MINUTES);
  }

  private void testSparkCompute() throws Exception {
    /**
     * stream --> transform --> sparkcompute --> tpfssink
     */
    String textsToClassify = "textsToClassify";

    String script = "function transform(input, emitter, context) {" +
      "  parts = input.body.split(',');" +
      "  text = parts[0];" +
      "  input.boolField = parts[1];" +
      "  emitter.emit(input);" +
      "}";

    Schema simpleMessageSchema =
      Schema.recordOf("simpleMessage",
                      Schema.Field.of(LogisticRegressionSpamMessage.TEXT_FIELD, Schema.of(Schema.Type.STRING)),
                      Schema.Field.of(LogisticRegressionSpamMessage.READ_FIELD, Schema.of(Schema.Type.STRING)));

    Map<String, String> sourceProp = ImmutableMap.of("name", textsToClassify,
                                                     "duration", "1d",
                                                     "format", "csv",
                                                     "schema", simpleMessageSchema.toString());

    Map<String, String> transformProp = ImmutableMap.of("script", script,
                                                        "schema", LogisticRegressionSpamMessage.SCHEMA.toString());

    String fieldsToClassify = LogisticRegressionSpamMessage.TEXT_FIELD + "," + LogisticRegressionSpamMessage.READ_FIELD;
    ETLPlugin transformPlugin = new ETLPlugin("JavaScript", Transform.PLUGIN_TYPE, transformProp, null);

    Map<String, String> sparkProp = ImmutableMap.of("fileSetName", "modelFileSet",
                                                    "path", "output",
                                                    "fieldsToClassify", fieldsToClassify,
                                                    "predictionField",
                                                    LogisticRegressionSpamMessage.SPAM_PREDICTION_FIELD);

    ETLPlugin sparkPlugin = new ETLPlugin("LogisticRegressionClassifier", SparkCompute.PLUGIN_TYPE, sparkProp, null);

    Map<String, String> sinkProp = ImmutableMap.of("name", "classifiedTexts",
                                                   "schema", LogisticRegressionSpamMessage.SCHEMA.toString());

    ETLPlugin sinkPlugin = new ETLPlugin("TPFSParquet", BatchSink.PLUGIN_TYPE, sinkProp, null);

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source", new ETLPlugin("Stream", BatchSource.PLUGIN_TYPE, sourceProp, null)))
      .addStage(new ETLStage("eventParser", transformPlugin))
      .addStage(new ETLStage("sparkCompute", sparkPlugin))
      .addStage(new ETLStage("sink", sinkPlugin))
      .addConnection("source", "eventParser")
      .addConnection("eventParser", "sparkCompute")
      .addConnection("sparkCompute", "sink")
      .setDriverResources(new Resources(1024))
      .setResources(new Resources(1024))
      .build();

    Id.Application app = Id.Application.from(Id.Namespace.DEFAULT, "SpamClassifier");
    ApplicationManager appManager = deployApplication(app, getBatchAppRequestV2(etlConfig));

    // write some some messages to be classified
    StreamManager streamManager =
      getTestManager().getStreamManager(Id.Stream.from(Id.Namespace.DEFAULT, textsToClassify));
    streamManager.send("how are you doing today");
    streamManager.send("free money money");
    streamManager.send("what are you doing today");
    streamManager.send("genuine report");

    // manually trigger the pipeline
    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForFinish(10, TimeUnit.MINUTES);

    Set<LogisticRegressionSpamMessage> expected = new HashSet<>();
    expected.add(new LogisticRegressionSpamMessage("how are you doing today", "no", 0.0));
    // only 'free money money' should be predicated as spam
    expected.add(new LogisticRegressionSpamMessage("free money money", "yes", 1.0));
    expected.add(new LogisticRegressionSpamMessage("what are you doing today", "no", 0.0));
    expected.add(new LogisticRegressionSpamMessage("genuine report", "no", 0.0));

    Assert.assertEquals(expected, getClassifiedMessages());
  }

  private Set<LogisticRegressionSpamMessage> getClassifiedMessages() throws ExecutionException, InterruptedException {
    QueryClient queryClient = new QueryClient(getClientConfig());
    ExploreExecutionResult exploreExecutionResult =
      queryClient.execute(TEST_NAMESPACE, "SELECT * FROM dataset_classifiedTexts").get();

    Set<LogisticRegressionSpamMessage> classifiedMessages = new HashSet<>();
    while (exploreExecutionResult.hasNext()) {
      List<Object> columns = exploreExecutionResult.next().getColumns();
      classifiedMessages.add(
        new LogisticRegressionSpamMessage((String) columns.get(1), (String) columns.get(2), (double) columns.get(0)));
    }
    return classifiedMessages;
  }
}

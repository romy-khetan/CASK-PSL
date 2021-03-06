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

package co.cask.hydrator.plugin.spark;


import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import co.cask.cdap.etl.api.batch.SparkPluginContext;
import co.cask.cdap.etl.api.batch.SparkSink;
import com.google.common.base.Preconditions;
import org.apache.avro.reflect.Nullable;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.regression.LabeledPoint;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Spark Sink plugin that trains a model based upon various labels in the structured record.
 * Writes this model to a file of a FileSet.
 */
@Plugin(type = SparkSink.PLUGIN_TYPE)
@Name(LogisticRegressionTrainer.PLUGIN_NAME)
@Description("Uses Logistic Regression to train a model based upon whether messages are spam or not.")
public class LogisticRegressionTrainer extends SparkSink<StructuredRecord> {
  public static final String PLUGIN_NAME = "LogisticRegressionTrainer";
  private static final int MAX_FEATURE_NUM = 100;

  private final Config config;

  public LogisticRegressionTrainer(Config config) {
    this.config = config;
  }

  /**
   * Configuration for the LogisticRegressionTrainer.
   */
  public static class Config extends PluginConfig {

    @Description("The name of the FileSet to save the model to.")
    private String fileSetName;

    @Description("Path of the FileSet to save the model to.")
    private String path;

    @Nullable
    @Description("A comma-separated sequence of fields that needs to be used for training.")
    private String featureFieldsToInclude;

    @Nullable
    @Description("A comma-separated sequence of fields that needs to be excluded from being used in training.")
    private String featureFieldsToExclude;

    @Description("The field from which to get the label. It must be of type double.")
    private String labelField;

    @Nullable
    @Description("The number of features to use in training the model. It must be of type integer and equal to the" +
      " number of features used in LogisticRegressionClassifier. The default value if none is provided will be" +
      " 100.")
    private final Integer numFeatures;

    @Nullable
    @Description("The number of classes to use in training the model. It must be of type integer. " +
      "The default value if none is provided will be 2.")
    private final Integer numClasses;

    public Config() {
      this.numFeatures = 100;
      this.numClasses = 2;
    }

    public Config(String fileSetName, String path, String featureFieldsToInclude, String labelField,
                  String featureFieldsToExclude, Integer numFeatures, Integer numClasses) {
      this.fileSetName = fileSetName;
      this.path = path;
      this.featureFieldsToInclude = featureFieldsToInclude;
      this.featureFieldsToExclude = featureFieldsToExclude;
      this.labelField = labelField;
      this.numFeatures = numFeatures;
      this.numClasses = numClasses;
    }
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    pipelineConfigurer.createDataset(config.fileSetName, FileSet.class);

    Schema inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();
    Preconditions.checkArgument(inputSchema != null, "Input Schema must be a known constant.");
    SparkUtils.validateLabelFieldForTrainer(inputSchema, config.labelField);
  }

  @Override
  public void prepareRun(SparkPluginContext context) throws Exception {
    // no-op; no need to do anything
  }

  @Override
  public void run(SparkExecutionPluginContext context, JavaRDD<StructuredRecord> input) throws Exception {
    final HashingTF tf = new HashingTF((config.numFeatures));

    if (!input.isEmpty()) {
      final Schema inputSchema = input.first().getSchema();

      final Map<String, Integer> featuresList = SparkUtils.getFeatureList(inputSchema,
                                                                          config.featureFieldsToInclude,
                                                                          config.featureFieldsToExclude,
                                                                          config.labelField);

      JavaRDD<LabeledPoint> trainingData = input.map(new Function<StructuredRecord, LabeledPoint>() {
        @Override
        public LabeledPoint call(StructuredRecord record) throws Exception {
          List<Object> result = new ArrayList<>();
          for (String column : featuresList.keySet()) {
            if (inputSchema.getField(column).getSchema().getType() != Schema.Type.DOUBLE) {
              result.add(String.valueOf(record.get(column)));
            } else {
              result.add(record.get(column));
            }
          }
          return new LabeledPoint((Double) record.get(config.labelField), tf.transform(result));
        }
      });

      trainingData.cache();

      final LogisticRegressionModel model = new LogisticRegressionWithLBFGS()
        .setNumClasses(config.numClasses)
        .run(trainingData.rdd());

      // save the model to a file in the output FileSet
      JavaSparkContext sparkContext = context.getSparkContext();
      FileSet outputFS = context.getDataset(config.fileSetName);
      model.save(JavaSparkContext.toSparkContext(sparkContext),
                 outputFS.getBaseLocation().append(config.path).toURI().getPath());
    }
  }
}

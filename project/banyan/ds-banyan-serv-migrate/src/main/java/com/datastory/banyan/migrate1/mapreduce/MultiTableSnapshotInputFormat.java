/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datastory.banyan.migrate1.mapreduce;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil.initTableMapperJob;
import static org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil.resetCacheConfig;

/**
 * MultiTableSnapshotInputFormat generalizes
 * {@link org.apache.hadoop.hbase.mapreduce.TableSnapshotInputFormat}
 * allowing a MapReduce job to run over one or more table snapshots, with one or more scans
 * configured for each.
 * Internally, the input format delegates to
 * {@link org.apache.hadoop.hbase.mapreduce.TableSnapshotInputFormat}
 * and thus has the same performance advantages;
 * see {@link org.apache.hadoop.hbase.mapreduce.TableSnapshotInputFormat} for
 * more details.
 * Usage is similar to TableSnapshotInputFormat, with the following exception:
 * initMultiTableSnapshotMapperJob takes in a map
 * from snapshot name to a collection of scans. For each snapshot in the map, each corresponding
 * scan will be applied;
 * the overall dataset for the job is defined by the concatenation of the regions and tables
 * included in each snapshot/scan
 * pair.
 * (java.util.Map, Class, Class, Class, org.apache.hadoop.mapreduce.Job, boolean, org.apache
 * .hadoop.fs.Path)}
 * can be used to configure the job.
 * <pre>{@code
 * Job job = new Job(conf);
 * Map<String, Collection<Scan>> snapshotScans = ImmutableMap.of(
 *    "snapshot1", ImmutableList.of(new Scan(Bytes.toBytes("a"), Bytes.toBytes("b"))),
 *    "snapshot2", ImmutableList.of(new Scan(Bytes.toBytes("1"), Bytes.toBytes("2")))
 * );
 * Path restoreDir = new Path("/tmp/snapshot_restore_dir")
 * TableMapReduceUtil.initTableSnapshotMapperJob(
 *     snapshotScans, MyTableMapper.class, MyMapKeyOutput.class,
 *      MyMapOutputValueWritable.class, job, true, restoreDir);
 * }
 * </pre>
 * Internally, this input format restores each snapshot into a subdirectory of the given tmp
 * directory. Input splits and
 * record readers are created as described in {@link org.apache.hadoop.hbase.mapreduce
 * .TableSnapshotInputFormat}
 * (one per region).
 * See {@link org.apache.hadoop.hbase.mapreduce.TableSnapshotInputFormat} for more notes on
 * permissioning; the
 * same caveats apply here.
 *
 * @see org.apache.hadoop.hbase.mapreduce.TableSnapshotInputFormat
 * @see org.apache.hadoop.hbase.client.TableSnapshotScanner
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class MultiTableSnapshotInputFormat extends TableSnapshotInputFormat {

  private final MultiTableSnapshotInputFormatImpl delegate;

  public MultiTableSnapshotInputFormat() {
    this.delegate = new MultiTableSnapshotInputFormatImpl();
  }

  @Override
  public List<InputSplit> getSplits(JobContext jobContext)
      throws IOException, InterruptedException {
    List<TableSnapshotInputFormatImpl.InputSplit> splits =
        delegate.getSplits(jobContext.getConfiguration());
    List<InputSplit> rtn = Lists.newArrayListWithCapacity(splits.size());

    for (TableSnapshotInputFormatImpl.InputSplit split : splits) {
      rtn.add(new TableSnapshotInputFormat.TableSnapshotRegionSplit(split));
    }

    return rtn;
  }

  public static void setInput(Configuration configuration,
                              Map<String, Collection<Scan>> snapshotScans, Path tmpRestoreDir) throws IOException {
    new MultiTableSnapshotInputFormatImpl().setInput(configuration, snapshotScans, tmpRestoreDir);
  }

  /**
   * Sets up the job for reading from one or more table snapshots, with one or more scans
   * per snapshot.
   * It bypasses hbase servers and read directly from snapshot files.
   *
   * @param snapshotScans     map of snapshot name to scans on that snapshot.
   * @param mapper            The mapper class to use.
   * @param outputKeyClass    The class of the output key.
   * @param outputValueClass  The class of the output value.
   * @param job               The current job to adjust.  Make sure the passed job is
   *                          carrying all necessary HBase configuration.
   * @param addDependencyJars upload HBase jars and jars for any of the configured
   *                          job classes via the distributed cache (tmpjars).
   * @param tmpRestoreDir a temporary directory to copy the snapshot files into. Current user should
   *                          have write permissions to this directory, and this should not be a subdirectory of rootdir.
   */
  public static void initMultiTableSnapshotMapperJob(Map<String, Collection<Scan>> snapshotScans,
                                                     Class<? extends TableMapper> mapper, Class<?> outputKeyClass, Class<?> outputValueClass,
                                                     Job job, boolean addDependencyJars, Path tmpRestoreDir) throws IOException {

    MultiTableSnapshotInputFormat.setInput(job.getConfiguration(), snapshotScans, tmpRestoreDir);

    job.setInputFormatClass(MultiTableSnapshotInputFormat.class);
    if (outputValueClass != null) {
      job.setMapOutputValueClass(outputValueClass);
    }
    if (outputKeyClass != null) {
      job.setMapOutputKeyClass(outputKeyClass);
    }
    job.setMapperClass(mapper);
    Configuration conf = job.getConfiguration();
    HBaseConfiguration.merge(conf, HBaseConfiguration.create(conf));

    if (addDependencyJars) {
      TableMapReduceUtil.addDependencyJars(job);
    }

    resetCacheConfig(job.getConfiguration());
  }

  /**
   * Sets up the job for reading from a table snapshot. It bypasses hbase servers
   * and read directly from snapshot files.
   *
   * @param snapshotName The name of the snapshot (of a table) to read from.
   * @param scan  The scan instance with the columns, time range etc.
   * @param mapper  The mapper class to use.
   * @param outputKeyClass  The class of the output key.
   * @param outputValueClass  The class of the output value.
   * @param job  The current job to adjust.  Make sure the passed job is
   * carrying all necessary HBase configuration.
   * @param addDependencyJars upload HBase jars and jars for any of the configured
   *           job classes via the distributed cache (tmpjars).
   *
   * @param tmpRestoreDir a temporary directory to copy the snapshot files into. Current user should
   * have write permissions to this directory, and this should not be a subdirectory of rootdir.
   * After the job is finished, restore directory can be deleted.
   * @throws IOException When setting up the details fails.
   * @see TableSnapshotInputFormat
   */
  public static void initTableSnapshotMapperJob(String snapshotName, Scan scan,
                                                Class<? extends TableMapper> mapper,
                                                Class<?> outputKeyClass,
                                                Class<?> outputValueClass, Job job,
                                                boolean addDependencyJars, Path tmpRestoreDir)
          throws IOException {
    TableSnapshotInputFormat.setInput(job, snapshotName, tmpRestoreDir);
    initTableMapperJob(snapshotName, scan, mapper, outputKeyClass,
            outputValueClass, job, addDependencyJars, false, TableSnapshotInputFormat.class);
    resetCacheConfig(job.getConfiguration());
  }
}

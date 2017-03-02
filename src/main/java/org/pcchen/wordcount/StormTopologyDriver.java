package org.pcchen.wordcount;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import org.apache.commons.lang.math.RandomUtils;

import java.util.Random;

/**
 * storm的topology主类
 * Created by ceek on 2017-02-27 11:55.
 */
public class StormTopologyDriver {
    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("localFileSpout", new LocalFileSpout());
        topologyBuilder.setBolt("splitBolt", new SplitBolt()).shuffleGrouping("localFileSpout");
        topologyBuilder.setBolt("wordCountAndPrintBolt", new WordCountAndPrintBolt()).shuffleGrouping("splitBolt");

        StormTopology topology = topologyBuilder.createTopology();

        Config config = new Config();
//        config.setNumWorkers(2);
        //集群本地提交模式
//        LocalCluster localcluster = new LocalCluster();
//        localcluster.submitTopology("wordcount1", config, topology);
        StormSubmitter.submitTopology(""+ RandomUtils.nextInt(10), config, topology);
    }
}

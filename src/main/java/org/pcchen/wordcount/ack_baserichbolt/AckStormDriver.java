package org.pcchen.wordcount.ack_baserichbolt;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;

/**
 * 测试ack机制的topology
 * Created by ceek on 2017-03-06 1:02.
 */
public class AckStormDriver {
    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("ack_spout", new AckSpout());
        builder.setBolt("bolt1", new Bolt1()).shuffleGrouping("ack_spout");
        builder.setBolt("bolt2", new Bolt2()).shuffleGrouping("bolt1");
        builder.setBolt("bolt3", new Bolt3()).shuffleGrouping("bolt2");

        StormTopology topology = builder.createTopology();
        Config conf = new Config();

        LocalCluster localcluster = new LocalCluster();
        localcluster.submitTopology("ack_topology", conf, topology);
    }
}

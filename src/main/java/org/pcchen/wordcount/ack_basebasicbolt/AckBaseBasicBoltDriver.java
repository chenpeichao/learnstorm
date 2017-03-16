package org.pcchen.wordcount.ack_basebasicbolt;


import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;

/**
 * 测试ack机制在basebasicbolt模式下的主类
 * 通过throw FailedException进行消息失败处理
 * Created by ceek on  2017-03-06 15:38
 */
public class AckBaseBasicBoltDriver {
    public static void main(String[] args) {
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("ack_spout", new AckBaseBasicBoltSpout(), 1);
        topologyBuilder.setBolt("Bolt1", new Bolt1(), 1).shuffleGrouping("ack_spout");
        topologyBuilder.setBolt("Bolt2", new Bolt2(), 1).shuffleGrouping("Bolt1");
        topologyBuilder.setBolt("Bolt3", new Bolt2(), 1).shuffleGrouping("Bolt2");

        StormTopology topology = topologyBuilder.createTopology();
        Config config = new Config();

        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("ack_basebasicbolt", config, topology);
    }
}

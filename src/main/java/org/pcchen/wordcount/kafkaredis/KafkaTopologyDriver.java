package org.pcchen.wordcount.kafkaredis;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import storm.kafka.*;
import storm.kafka.KafkaSpout;

        import java.util.prefs.BackingStoreException;

/**
 * storm读取kafka中的数据的主题类topology
 * Created by ceek on 2017-03-15 23:22.
 */
public class KafkaTopologyDriver  {
    public static void main(String[] args) {
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("kafkaSpout", new KafkaSpout(new SpoutConfig(new ZkHosts("hadoop01:2181"), "kafka_storm_redis", "/storm", "1")));
        topologyBuilder.setBolt("splitBolt", new KafkaSplitBolt()).shuffleGrouping("kafkaSpout");
        topologyBuilder.setBolt("coutBolt", new KafkaCoutBolt()).shuffleGrouping("splitBolt");

        Config conf = new Config();

        StormTopology topology = topologyBuilder.createTopology();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("kafkaCountWord", conf, topology);
    }
}

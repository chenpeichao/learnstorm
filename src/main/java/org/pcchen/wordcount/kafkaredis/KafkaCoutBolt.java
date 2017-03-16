package org.pcchen.wordcount.kafkaredis;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import org.apache.commons.lang.StringUtils;
import org.pcchen.wordcount.JedisUtil;
import redis.clients.jedis.Jedis;

import java.util.Map;

/**
 * 对传递过来的切割后的字符串进行统计，并输出到redis
 * Created by ceek on  2017-03-16 10:41
 */
public class KafkaCoutBolt extends BaseRichBolt{
    private Jedis jedis;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        jedis = JedisTool.getJedis();
    }

    @Override
    public void execute(Tuple tuple) {
//        String line = tuple.getString(0);
        String line  = (String) tuple.getValueByField("splitLine");
        tuple.getValueByField("splitLine");
        if(StringUtils.isNotBlank(jedis.get(line))) {
            jedis.incr(line);
        } else {
            jedis.set(line, "1");
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}

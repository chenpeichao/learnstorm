package org.pcchen.wordcount;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import org.apache.commons.lang.StringUtils;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.Map;

/**
 * 对传递过来的已经分割的字符串和个数进行汇总统计以及输出
 * Created by ceek on 2017-02-27 20:55.
 */
public class WordCountAndPrintBolt extends BaseBasicBolt{
    private Map<String, String> wordCountMap = new HashMap<String, String>();


//    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String word = (String)tuple.getValueByField("word");
        if(!StringUtils.isNotBlank(wordCountMap.get(word))) {
            wordCountMap.put(word, 1 + "");
        } else {
            Integer count = Integer.parseInt(wordCountMap.get(word));
            wordCountMap.put(word, (count+1) +"");
        }
//        System.err.println(wordCountMap);
        JedisUtil.save("wordcount", wordCountMap);
    }

    /*//不知道为何此处不执行
    public void cleanup() {
        System.err.println(wordCountMap);
    }*/


    /**
     * 因为不需要向下面bolt再传递，所以此处不需要实现
     */
//    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zleying.common.RedisUtils;
import com.zleying.model.TopicPartitionOffset;
import com.zleying.model.TopicPartitionOffsets;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestRedis {

    @Test
    public void testfromOffsets() throws IOException {
        Jedis jedis = RedisUtils.getJedis();
        Map<TopicPartition,Long> fromOffsets=new HashMap<TopicPartition,Long>();
        String offsets = jedis.get("sparkstreaming-kafka-test-offset");
        if (offsets!=null){
            ObjectMapper mapper = new ObjectMapper();
            List<TopicPartitionOffset> topicPartitonOffsetsList  = mapper.readValue(offsets, new TypeReference<List<TopicPartitionOffset>>() { });
            for (TopicPartitionOffset topicPartitonOffset : topicPartitonOffsetsList) {
                String topic=topicPartitonOffset.getTopic();
                int partition = topicPartitonOffset.getPartition();
                long untilOffset = topicPartitonOffset.getUntilOffset();
                System.out.println(topic+","+partition+","+untilOffset);
                fromOffsets.put(new TopicPartition(topic,partition),untilOffset);
            }
        }
    }

}

package redis.clients.jedis;


import org.junit.Test;

import java.util.*;
import java.util.logging.Logger;

public class JedisSentinelSlavePoolTest {




    protected Logger log = Logger.getLogger(getClass().getName());

    @Test
    public void sentinel() {
        // 建立连接池配置参数
        JedisPoolConfig config = new JedisPoolConfig();
        // 设置最大连接数
        config.setMaxTotal(10000);
        // 设置最大阻塞时间，记住是毫秒数milliseconds
        config.setMaxWaitMillis(1000);
        // 设置空间连接
        config.setMaxIdle(500);
        // jedis实例是否可用
        config.setTestOnBorrow(true);
        // 创建连接池
//      pool = new JedisPool(config, prop.getProperty("ADDR"), StringUtil.nullToInteger(prop.getProperty("PORT")), StringUtil.nullToInteger(prop.getProperty("TIMEOUT")));// 线程数量限制，IP地址，端口，超时时间
        //获取redis密码

        String masterName = "mymaster";
        Set<String> sentinels = new HashSet<String>();
        sentinels.add("10.171.64.253:26369");
        sentinels.add("10.174.8.93:26369");
        sentinels.add("10.45.141.174:26369");
        JedisSentinelSlavePool pool = new JedisSentinelSlavePool(masterName, sentinels, config);
//        Jedis jedis = pool.getResource();
//
//
//        jedis.set("test11", "test11");
//        jedis.close();

        while (true) {
            log.info("主节点：" + pool.getCurrentHostMaster().toString());
            log.info("从节点：" + pool.getCurrentSlaves().toString());
            try {
                Jedis master = pool.getResource();
                log.info("master" + master.getClient().getHost() + ":" + master.getClient().getPort());
                Jedis slave = pool.getSlaveResource();
                log.info("salve" + slave.getClient().getHost() + ":" + slave.getClient().getPort());
//            for (int i = 0; i < 1000; i++) {
//                master.set(String.valueOf(i), String.valueOf(i));
//                slave.get(String.valueOf(i));
//            }
                master.close();
                slave.close();

            } catch (Exception e) {
                e.printStackTrace();
            }
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }


    @Test
    public void testEquals() {
        HostAndPort hostAndPort = new HostAndPort("10.45.141.174", 6369);
        List<HostAndPort> hostAndPorts = Collections.synchronizedList(new ArrayList<HostAndPort>());
        hostAndPorts.add(hostAndPort);
        Boolean result = hostAndPorts.contains(new HostAndPort("10.45.141.174", 6369));
        log.info(result.toString());
    }


}


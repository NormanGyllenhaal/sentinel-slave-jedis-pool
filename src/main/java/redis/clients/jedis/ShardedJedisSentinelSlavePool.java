package redis.clients.jedis;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.util.Hashing;
import redis.clients.util.Pool;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

public class ShardedJedisSentinelSlavePool extends Pool<ShardedJedis> {


    private List<SlavePool<ShardedJedis>> slavePools = new CopyOnWriteArrayList<>();


    private GenericObjectPoolConfig poolConfig;

    protected int timeout = Protocol.DEFAULT_TIMEOUT;

    protected String password;

    protected int database = Protocol.DEFAULT_DATABASE;

    private Set<MasterListener> masterListeners = new HashSet<>();

    protected Logger log = LoggerFactory.getLogger(ShardedJedisSentinelSlavePool.class);


    protected ThreadLocalRandom random = ThreadLocalRandom.current();

    private List<Map<String, JedisShardInfo>> liveSlaves = new CopyOnWriteArrayList<>();


    public ShardedJedisSentinelSlavePool(String masterName, Map<String, Set<String>> map,
                                         final GenericObjectPoolConfig poolConfig) {
        this(masterName, map, poolConfig, Protocol.DEFAULT_TIMEOUT, null,
                Protocol.DEFAULT_DATABASE);
    }


    public ShardedJedisSentinelSlavePool(String masterName, Map<String, Set<String>> map,
                                         final GenericObjectPoolConfig poolConfig, int timeout,
                                         final String password, final int database) {
        this.poolConfig = poolConfig;
        this.timeout = timeout;
        this.password = password;
        this.database = database;
        List<Map<String, JedisShardInfo>> list = initSentinels(map, masterName);
        initPool(list.get(0));
        initSlavePools(list.subList(1, list.size()));
    }


    public void reload(Map<String, Set<String>> map, String masterName) {
        log.info("重新加载shared pool");
        List<Map<String, JedisShardInfo>> list = initSentinels(map, masterName);
        initPool(list.get(0));
        initSlavePools(list.subList(1, list.size()));
    }


    private volatile Map<String, JedisShardInfo> currentHostMaster;


    private volatile List<Map<String, JedisShardInfo>> currentSlaves = new CopyOnWriteArrayList<>();


    public void destroy() {
        for (MasterListener m : masterListeners) {
            m.shutdown();
        }
        for (SlavePool<ShardedJedis> pool : slavePools) {
            pool.close();
        }
        slavePools.clear();
        super.destroy();
    }


//    public Map<String, JedisShardInfo> getCurrentHostMaster() {
//        return currentHostMaster;
//    }
//
//
//    public List<Map<String, JedisShardInfo>> getCurrentSlaves() {
//        return currentSlaves;
//    }
//
//
//    public List<Map<String, JedisShardInfo>> getLiveSlaves() {
//        return liveSlaves;
//    }


    public ShardedJedis getSlaveResource() {
        try {
            if (slavePools.size() > 0) {
                log.debug("从节点读");
                SlavePool<ShardedJedis> pool = slavePools.get(random.nextInt(slavePools.size()));
                ShardedJedis jedis = pool.getResource();
                jedis.setDataSource(pool);
                return jedis;
            } else {
                log.debug("所有从节点关闭，从主节点获取实例");
                ShardedJedis jedis = super.getResource();
                jedis.setDataSource(this);
                return jedis;
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new JedisConnectionException("Could not get a resource from the pool", e);
        }
    }


    @Override
    public ShardedJedis getResource() {
        ShardedJedis jedis = super.getResource();
        jedis.setDataSource(this);
        return jedis;
    }


    @Override
    @Deprecated
    public void returnBrokenResource(final ShardedJedis resource) {
        if (resource != null) {
            returnBrokenResourceObject(resource);
        }
    }

    /**
     * @deprecated starting from Jedis 3.0 this method will not be exposed. Resource cleanup should be
     * done using @see {@link redis.clients.jedis.Jedis#close()}
     */
    @Override
    @Deprecated
    public void returnResource(final ShardedJedis resource) {
        if (resource != null) {
            resource.resetState();
            returnResourceObject(resource);
        }
    }


    private void initPool(Map<String, JedisShardInfo> masters) {
        currentHostMaster = masters;
        log.info("创建 master jedis shard pool " + masters);
        initPool(poolConfig, new ShardedJedisFactory(new ArrayList<>(masters.values()), Hashing.MURMUR_HASH, null));
    }


    private synchronized void reloadMasterPool(JedisShardInfo newJedisShardInfo) {
        try {
            JedisShardInfo jedisShardInfo = currentHostMaster.get(newJedisShardInfo.getName());
            if (jedisShardInfo.getPort() != newJedisShardInfo.getPort() || !jedisShardInfo.getHost().equals(newJedisShardInfo.getHost())) {
                currentHostMaster.put(newJedisShardInfo.getName(), newJedisShardInfo);
                initPool(poolConfig, new ShardedJedisFactory(new ArrayList<>(currentHostMaster.values()), Hashing.MURMUR_HASH, null));
                log.info("redis 主节点重新加载成功  {} ", currentHostMaster.toString());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private synchronized void addSlavePool(JedisShardInfo slave) {
        log.info("当前redis从节点：" + currentSlaves);
        if (!isExistJedisShaard(slave, currentSlaves)) {
            Map<String, JedisShardInfo> map = getLiveSlave(slave);
            if (map != null) {
                log.info("添加一个redis 从 " + map);
                currentSlaves.add(map);
                SlavePool<ShardedJedis> slavePool = new SlavePool<>(poolConfig, new ShardedJedisFactory(new ArrayList<JedisShardInfo>(map.values()), Hashing.MURMUR_HASH, null));
                slavePools.add(slavePool);
            }
        }
    }


    private boolean isExistJedisShaard(JedisShardInfo slave, List<Map<String, JedisShardInfo>> slaves) {
        for (Map<String, JedisShardInfo> map : slaves) {
            JedisShardInfo jedisShardInfo = map.get(slave.getName());
            if (jedisShardInfo != null) {
                if (jedisShardInfo.getPort() == slave.getPort() && jedisShardInfo.getHost().equals(slave.getHost())) {
                    return true;
                }
            }
        }
        return false;
    }

    private synchronized void removeSlavePool(JedisShardInfo slave) {
        for (int i = 0; i < currentSlaves.size(); i++) {
            Map<String, JedisShardInfo> next = currentSlaves.get(i);
            JedisShardInfo jedisShardInfo = next.get(slave.getName());
            if (jedisShardInfo != null) {
                if (jedisShardInfo.getPort() == slave.getPort() && jedisShardInfo.getHost().equals(slave.getHost())) {
                    log.info("移除从节点分片 " + jedisShardInfo.getName() + "  " + jedisShardInfo);
                    if (next.size() > 1) {
                        next.remove(slave.getName());
                        addLiveSlave(next);
                    }
                    currentSlaves.remove(i);
                }
            }
        }
        removeLiveSlave(slave);
        log.info("----重新加载从redis pool----");
        initSlavePools(currentSlaves);
    }


    private void addLiveSlave(Map<String, JedisShardInfo> slaves) {
        log.info("增加：当前备用节点：" + liveSlaves);
        for (Map.Entry<String, JedisShardInfo> entry : slaves.entrySet()) {
            for (Map<String, JedisShardInfo> map : liveSlaves) {
                JedisShardInfo jedisShardInfo = map.get(entry.getKey());
                if (jedisShardInfo != null) {
                    if (jedisShardInfo.getPort() == entry.getValue().getPort() && jedisShardInfo.getHost().equals(entry.getValue().getHost())) {
                        return;
                    }
                }
            }
        }
        log.info("加入备用从节点： " + slaves);
        liveSlaves.add(slaves);
    }


    private void addLiveSlave(JedisShardInfo slave) {
        if (!isExistJedisShaard(slave, liveSlaves)) {
            boolean flag = false;
            if (liveSlaves.size() > 0) {
                for (Map<String, JedisShardInfo> map : liveSlaves) {
                    if (map.size() < currentHostMaster.size()) {
                        if (map.get(slave.getName()) == null) {
                            map.put(slave.getName(), slave);
                            flag = true;
                            break;
                        }
                    }
                }
            }
            if (!flag) {
                Map<String, JedisShardInfo> map = new ConcurrentHashMap<>();
                map.put(slave.getName(), slave);
                liveSlaves.add(map);
            }
        }
    }

    private void removeLiveSlave(JedisShardInfo slave) {
        log.info("删除：当前备用节点：" + liveSlaves);
        for (int i = 0; i < liveSlaves.size(); i++) {
            Map<String, JedisShardInfo> next = liveSlaves.get(i);
            JedisShardInfo jedisShardInfo = next.get(slave.getName());
            if (jedisShardInfo != null) {
                if (jedisShardInfo.getPort() == slave.getPort() && jedisShardInfo.getHost().equals(slave.getHost())) {
                    log.info("移除备用的从节点" + jedisShardInfo);
                    next.remove(slave.getName());
                    if (next.size() == 0) {
                        liveSlaves.remove(i);
                    }
                }
            }
        }
    }


    private Map<String, JedisShardInfo> getLiveSlave(JedisShardInfo slave) {
        Map<String, JedisShardInfo> shardJedis = null;
        addLiveSlave(slave);
        log.info("当前备用节点：" + liveSlaves.toString());
        for (int i = 0; i < liveSlaves.size(); i++) {
            Map<String, JedisShardInfo> map = liveSlaves.get(i);
            if (map.size() == currentHostMaster.size()) {
                shardJedis = map;
                liveSlaves.remove(i);
            }
        }
        return shardJedis;
    }

    private void initSlavePools(List<Map<String, JedisShardInfo>> slaves) {
        currentSlaves = slaves;
        if (this.slavePools != null && this.slavePools.size() > 0) {
            for (SlavePool<ShardedJedis> pool : slavePools) {
                pool.close();
            }
            slavePools.clear();
            log.info("销毁旧的redis连接池");
        }
        if (slaves.size() > 0) {
            for (Map<String, JedisShardInfo> slave : slaves) {
                SlavePool<ShardedJedis> slavePool = new SlavePool<>(poolConfig, new ShardedJedisFactory(new ArrayList<>(slave.values()), Hashing.MURMUR_HASH, null));
                slavePools.add(slavePool);
            }
            log.info("slave pool 创建成功 {}", slaves);
        }
    }


    private List<Map<String, JedisShardInfo>> initSentinels(Map<String, Set<String>> map, final String masterName) {

        List<Map<String, JedisShardInfo>> masterSlaves = new CopyOnWriteArrayList<>();

        Map<String, JedisShardInfo> masterMap = new ConcurrentHashMap<>();

        Map<String, List<JedisShardInfo>> slaveMap = new ConcurrentHashMap<>();

        while (true) {
            log.info("对redis 主从分别进行分片");
            for (Map.Entry<String, Set<String>> entry : map.entrySet()) {
                String name = entry.getKey();
                Set<String> sentinels = entry.getValue();
                for (String sentinel : sentinels) {
                    final HostAndPort hap = toHostAndPort(Arrays.asList(sentinel.split(":")));
                    log.info("连接哨兵：" + hap);
                    Jedis jedis = new Jedis(hap.getHost(), hap.getPort());
                    JedisShardInfo jedisShardInfo = toJedisShardInfo(jedis.sentinelGetMasterAddrByName(masterName), name);
                    log.info("-----redis master----" + jedisShardInfo);
                    masterMap.putIfAbsent(name, jedisShardInfo);
                    List<JedisShardInfo> slaveList = toList(jedis, masterName, name);
                    log.info("--------哨兵" + sentinel + "获取redis的从节点为---------:" + slaveList);
                    slaveMap.putIfAbsent(name, slaveList);
                    jedis.disconnect();
                }
            }
            masterSlaves.add(masterMap);
            int count = 0;
            for (Map.Entry<String, List<JedisShardInfo>> entry : slaveMap.entrySet()) {
                if (entry.getValue().size() > count) {
                    count = entry.getValue().size();
                }
            }
            if (count > 0) {
                for (int i = 0; i < count; i++) {
                    Map<String, JedisShardInfo> jedisShardInfos = new ConcurrentHashMap<>();
                    for (Map.Entry<String, List<JedisShardInfo>> entry : slaveMap.entrySet()) {
                        JedisShardInfo jedisShardInfo;
                        if (entry.getValue().size() > 0) {
                            if (entry.getValue().size() == count) {
                                jedisShardInfo = entry.getValue().get(i);
                            } else {
                                jedisShardInfo = entry.getValue().get(0);
                            }
                        } else {
                            jedisShardInfo = masterMap.get(entry.getKey());
                        }
                        jedisShardInfos.put(entry.getKey(), jedisShardInfo);
                    }
                    masterSlaves.add(jedisShardInfos);
                }
            }

            if (masterSlaves.size() > 0) {
                break;
            }
            try {
                log.error("所有哨兵关闭，不能获取到master " + masterName);
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        log.info("启动哨兵监听...");
        for (Map.Entry<String, Set<String>> entry : map.entrySet()) {
            for (String sentinel : entry.getValue()) {
                final HostAndPort hap = toHostAndPort(Arrays.asList(sentinel.split(":")));
                ShardedJedisSentinelSlavePool.MasterListener masterListener = new ShardedJedisSentinelSlavePool.
                        MasterListener(masterName, hap.getHost(), hap.getPort(), entry.getKey());
                masterListeners.add(masterListener);
                masterListener.setName("sentinel-listener-thread:" + hap.getHost() + ":" + hap.getPort());
                masterListener.start();
            }
        }
        return masterSlaves;
    }


    private List<JedisShardInfo> toList(Jedis jedis, String masterName, String name) {
        List<Map<String, String>> maps = jedis.sentinelSlaves(masterName);
        List<JedisShardInfo> list = new ArrayList<>();
        if (maps != null && maps.size() > 0) {
            for (Map<String, String> map : maps) {
                String host = map.get("ip");
                String port = map.get("port");
                String flags = map.get("flags");
                if (!flags.contains("disconnected") && !flags.contains("s_down")) {
                    JedisShardInfo shardInfo = new JedisShardInfo(host, Integer.parseInt(port), name);
                    list.add(shardInfo);
                }
            }
        }
        return list;
    }


    private HostAndPort toHostAndPort(List<String> getMasterAddrByNameResult) {
        String host = getMasterAddrByNameResult.get(0);
        int port = Integer.parseInt(getMasterAddrByNameResult.get(1));
        return new HostAndPort(host, port);
    }

    private JedisShardInfo toJedisShardInfo(List<String> getMasterAddrByNameResult, String name) {
        String host = getMasterAddrByNameResult.get(0);
        int port = Integer.parseInt(getMasterAddrByNameResult.get(1));
        return new JedisShardInfo(host, port, name);
    }


    private static class ShardedJedisFactory implements PooledObjectFactory<ShardedJedis> {
        private List<JedisShardInfo> shards;
        private Hashing algo;
        private Pattern keyTagPattern;

        ShardedJedisFactory(List<JedisShardInfo> shards, Hashing algo,
                            Pattern keyTagPattern) {
            this.shards = shards;
            this.algo = algo;
            this.keyTagPattern = keyTagPattern;
        }

        @Override
        public PooledObject<ShardedJedis> makeObject() throws Exception {
            ShardedJedis jedis = new ShardedJedis(shards, algo, keyTagPattern);
            return new DefaultPooledObject<>(jedis);
        }

        @Override
        public void destroyObject(PooledObject<ShardedJedis> pooledShardedJedis)
                throws Exception {
            final ShardedJedis shardedJedis = pooledShardedJedis.getObject();
            for (Jedis jedis : shardedJedis.getAllShards()) {
                try {
                    try {
                        jedis.quit();
                    } catch (Exception e) {

                    }
                    jedis.disconnect();
                } catch (Exception e) {

                }
            }
        }

        @Override
        public boolean validateObject(
                PooledObject<ShardedJedis> pooledShardedJedis) {
            try {
                ShardedJedis jedis = pooledShardedJedis.getObject();
                for (Jedis shard : jedis.getAllShards()) {
                    if (!shard.ping().equals("PONG")) {
                        return false;
                    }
                }
                return true;
            } catch (Exception ex) {
                return false;
            }
        }

        @Override
        public void activateObject(PooledObject<ShardedJedis> p)
                throws Exception {

        }

        @Override
        public void passivateObject(PooledObject<ShardedJedis> p)
                throws Exception {

        }
    }


    protected class JedisPubSubAdapter extends JedisPubSub {
        @Override
        public void onMessage(String channel, String message) {
        }

        @Override
        public void onPMessage(String pattern, String channel, String message) {
        }

        @Override
        public void onPSubscribe(String pattern, int subscribedChannels) {
        }

        @Override
        public void onPUnsubscribe(String pattern, int subscribedChannels) {
        }

        @Override
        public void onSubscribe(String channel, int subscribedChannels) {
        }

        @Override
        public void onUnsubscribe(String channel, int subscribedChannels) {
        }
    }

    protected class MasterListener extends Thread {

        String masterName;
        protected String host;
        protected int port;
        /**
         * 分片名称
         */
        protected String name;
        long subscribeRetryWaitTimeMillis = 5000;
        protected Jedis j;
        AtomicBoolean running = new AtomicBoolean(false);


        MasterListener(String masterName, String host, int port, String name) {
            this.masterName = masterName;
            this.host = host;
            this.port = port;
            this.name = name;
        }


        public void run() {

            running.set(true);

            while (running.get()) {


                j = new Jedis(host, port);

                try {
                    if (!running.get()) {
                        break;
                    }
                    log.info("执行redis 订阅");
                    j.subscribe(new JedisPubSubAdapter() {
                        @Override
                        public void onMessage(String channel, String message) {
                            log.info(" 频道 {} 消息 {} 来自 {}:{}", channel, message, host, port);
                            if (channel.equals("+switch-master")) {
                                switchMaster(message);
                            } else if (channel.equals("+slave")) {
                                slave(message);
                            } else if (channel.equals("+sdown")) {
                                addSdown(message);
                            } else if (channel.equals("-sdown")) {
                                subtractSdown(message);
                            }

                        }
                    }, "+switch-master", "+sdown", "-sdown", "+slave");

                } catch (JedisConnectionException e) {

                    if (running.get()) {
                        log.error("Lost connection to Sentinel at " + host
                                + ":" + port
                                + ". Sleeping 5000ms and retrying.");
                        try {
                            Thread.sleep(subscribeRetryWaitTimeMillis);
                        } catch (InterruptedException e1) {
                            e1.printStackTrace();
                        }
                    } else {
                        log.error("Unsubscribing from Sentinel at " + host + ":"
                                + port);
                    }
                } finally {
                    j.close();
                }
            }
        }

        void shutdown() {
            try {
                log.info("Shutting down listener on " + host + ":" + port);
                running.set(false);
                // This isn't good, the Jedis object is not thread safe
                j.disconnect();
            } catch (Exception e) {
                log.error("Caught exception while shutting down: " + e.getMessage());
            }
        }


        private void switchMaster(String message) {
            String[] switchMasterMsg = message.split(" ");

            if (switchMasterMsg.length > 3) {

                if (masterName.equals(switchMasterMsg[0])) {
                    reloadMasterPool(
                            toJedisShardInfo(Arrays.asList(
                                    switchMasterMsg[3],
                                    switchMasterMsg[4]), name));
                    removeSlavePool(toJedisShardInfo(Arrays.asList(switchMasterMsg[3], switchMasterMsg[4]), name));
                } else {
                    log.info("Ignoring message on +switch-master for master name "
                            + switchMasterMsg[0]
                            + ", our master name is "
                            + masterName);
                }

            } else {
                log.info("Invalid message received on Sentinel "
                        + host
                        + ":"
                        + port
                        + " on channel +switch-master: "
                        + message);
            }
        }


        private void slave(String message) {
            String[] split = message.split(" ");
            if (split.length > 2) {
                JedisShardInfo jedisShardInfo = new JedisShardInfo(split[2], Integer.parseInt(split[3]), name);
                addSlavePool(jedisShardInfo);
            } else {
                log.info("收到了无效的 +slave消息：" + message);
            }
        }


        private void addSdown(String message) {
            String[] split = message.split(" ");
            if (split.length > 2) {
                if (split[0].equals("slave")) {
                    JedisShardInfo jedisShardInfo = new JedisShardInfo(split[2], Integer.parseInt(split[3]), name);
                    log.info("slave:" + jedisShardInfo + "下线");
                    removeSlavePool(jedisShardInfo);
                } else {
                    log.info("非slave 下线消息");
                }
            } else {
                log.info("收到了无效的 +sdown消息：" + message);
            }
        }


        private void subtractSdown(String message) {
            String[] split = message.split(" ");

            if (split.length > 2) {
                if (split[0].equals("slave")) {
                    JedisShardInfo jedisShardInfo = new JedisShardInfo(split[2], Integer.parseInt(split[3]), name);
                    log.info("slave:" + jedisShardInfo + "上线");
                    addSlavePool(jedisShardInfo);
                } else {
                    log.info("非slave 上线消息");
                }
            } else {
                log.info("收到了无效的 -sdown消息：" + message);
            }
        }
    }


}

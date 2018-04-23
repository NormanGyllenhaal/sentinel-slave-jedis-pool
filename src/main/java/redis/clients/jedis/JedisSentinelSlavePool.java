package redis.clients.jedis;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.util.Pool;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

public class JedisSentinelSlavePool extends Pool<Jedis> {


    protected Map<String, GenericObjectPool<Jedis>> slavePools = new ConcurrentHashMap<String, GenericObjectPool<Jedis>>();
    ;

    protected GenericObjectPoolConfig poolConfig;

    protected int timeout = Protocol.DEFAULT_TIMEOUT;

    protected int connectionTimeout = Protocol.DEFAULT_TIMEOUT;
    protected int soTimeout = Protocol.DEFAULT_TIMEOUT;


    protected String clientName;

    protected String password;

    protected int database = Protocol.DEFAULT_DATABASE;

    protected Set<MasterListener> masterListeners = new HashSet<MasterListener>();

    protected Logger log = Logger.getLogger(getClass().getName());

    public JedisSentinelSlavePool(String masterName, Set<String> sentinels,
                                  final GenericObjectPoolConfig poolConfig) {
        this(masterName, sentinels, poolConfig, Protocol.DEFAULT_TIMEOUT, null,
                Protocol.DEFAULT_DATABASE);
    }

    public JedisSentinelSlavePool(String masterName, Set<String> sentinels) {
        this(masterName, sentinels, new GenericObjectPoolConfig(),
                Protocol.DEFAULT_TIMEOUT, null, Protocol.DEFAULT_DATABASE);
    }

    public JedisSentinelSlavePool(String masterName, Set<String> sentinels,
                                  String password) {
        this(masterName, sentinels, new GenericObjectPoolConfig(),
                Protocol.DEFAULT_TIMEOUT, password);
    }

    public JedisSentinelSlavePool(String masterName, Set<String> sentinels,
                                  final GenericObjectPoolConfig poolConfig, int timeout,
                                  final String password) {
        this(masterName, sentinels, poolConfig, timeout, password,
                Protocol.DEFAULT_DATABASE);
    }

    public JedisSentinelSlavePool(String masterName, Set<String> sentinels,
                                  final GenericObjectPoolConfig poolConfig, final int timeout) {
        this(masterName, sentinels, poolConfig, timeout, null,
                Protocol.DEFAULT_DATABASE);
    }

    public JedisSentinelSlavePool(String masterName, Set<String> sentinels,
                                  final GenericObjectPoolConfig poolConfig, final String password) {
        this(masterName, sentinels, poolConfig, Protocol.DEFAULT_TIMEOUT,
                password);
    }

    public JedisSentinelSlavePool(String masterName, Set<String> sentinels,
                                  final GenericObjectPoolConfig poolConfig, int timeout,
                                  final String password, final int database) {
        this.poolConfig = poolConfig;
        this.timeout = timeout;
        this.password = password;
        this.database = database;
        Map<String, Object> map = initSentinels(sentinels, masterName);
        initPool((HostAndPort) map.get("master"));
        List<HostAndPort> slaves = (List<HostAndPort>) map.get("slaves");
        initSlavePools(slaves);
    }

    public void returnBrokenResource(final Jedis resource) {
        returnBrokenResourceObject(resource);
    }

    public void returnResource(final Jedis resource) {
        resource.resetState();
        returnResourceObject(resource);
    }

    private volatile HostAndPort currentHostMaster;


    private volatile List<HostAndPort> currentSlaves = Collections.synchronizedList(new ArrayList<HostAndPort>());


    public void destroy() {
        for (JedisSentinelSlavePool.MasterListener m : masterListeners) {
            m.shutdown();
        }

        super.destroy();
    }

    public HostAndPort getCurrentHostMaster() {
        return currentHostMaster;
    }


    public List<HostAndPort> getCurrentSlaves() {
        return currentSlaves;
    }


    public Jedis getSlaveResource() {
        try {
            if (slavePools.size() > 0) {
                Random random = new Random();
                List<String> list = new ArrayList<String>(slavePools.keySet());
                String s = list.get(random.nextInt(list.size()));
                GenericObjectPool<Jedis> pool = slavePools.get(s);
                return pool.borrowObject();
            } else {
                return internalPool.borrowObject();
            }
        } catch (Exception e) {
            throw new JedisConnectionException("Could not get a resource from the pool", e);
        }
    }

    private void initPool(HostAndPort master) {
        if (!master.equals(currentHostMaster)) {
            currentHostMaster = master;
            log.info("Created JedisPool to master at " + master);
            initPool(poolConfig,
                    new JedisFactory(master.getHost(), master.getPort(), connectionTimeout,
                            soTimeout, password, database, clientName, false, null, null, null));
        }
    }


    private void addSlavePool(HostAndPort slave) {
        log.info("current redis slaves：" + currentSlaves);
        if (!currentSlaves.contains(slave)) {
            currentSlaves.add(slave);
            log.info("add a redis slave" + slave);
            GenericObjectPool<Jedis> pool = new GenericObjectPool<Jedis>( new JedisFactory(slave.getHost(), slave.getPort(), connectionTimeout,
                    soTimeout, password, database, clientName, false, null, null, null), poolConfig);
            slavePools.put(slave.getHost() + ":" + slave.getPort(), pool);
        } else {
            log.info("redis slave already contain：" + slave);
        }
    }


    private void removeSlavePool(HostAndPort slave) {
        if (currentSlaves.contains(slave)) {
            log.info("remove a redis slave");
            currentSlaves.remove(slave);
            slavePools.remove(slave.getHost() + ":" + slave.getPort());
        } else {
            log.info("redis slave not contain：" + slave);
        }
    }

    private void initSlavePools(List<HostAndPort> slaves) {
        if (slaves.size() > 0) {
            currentSlaves = slaves;
            if (this.slavePools != null && this.slavePools.size() > 0) {
                slavePools.clear();
            }
            log.info("init redis slaves :" + slaves);
            for (HostAndPort slave : slaves) {
                GenericObjectPool<Jedis> pool = new GenericObjectPool<Jedis>(new JedisFactory(slave.getHost(), slave.getPort(), connectionTimeout,
                        soTimeout, password, database, clientName, false, null, null, null), poolConfig);
                slavePools.put(slave.getHost() + ":" + slave.getPort(), pool);
            }
        }
    }

    private Map<String, Object> initSentinels(Set<String> sentinels,
                                              final String masterName) {


        HostAndPort master = null;
        boolean running = true;

        Map<String, Object> masterSlaves = new HashMap<String, Object>();

        outer:
        while (running) {

            log.info("Trying to find master from available Sentinels...");

            for (String sentinel : sentinels) {

                final HostAndPort hap = toHostAndPort(Arrays.asList(sentinel
                        .split(":")));

                log.info("Connecting to Sentinel " + hap);

                try {
                    Jedis jedis = new Jedis(hap.getHost(), hap.getPort());

                    if (master == null) {
                        master = toHostAndPort(jedis
                                .sentinelGetMasterAddrByName(masterName));
                        log.info("Found Redis master at " + master);
                        masterSlaves.put("master", master);
                        List<Map<String, String>> maps = jedis.sentinelSlaves(masterName);
                        if (maps != null && maps.size() > 0) {
                            List<HostAndPort> list = new ArrayList<HostAndPort>();
                            for (Map<String, String> map : maps) {
                                log.info("redis slave：" + map);
                                String host = map.get("ip");
                                String port = map.get("port");
                                String flags = map.get("flags");
                                if (!flags.contains("disconnected") && !flags.contains("s_down")) {
                                    HostAndPort hostAndPort = new HostAndPort(host, Integer.parseInt(port));
                                    log.info(hostAndPort.toString());
                                    list.add(hostAndPort);
                                }
                            }
                            masterSlaves.put("slaves", list);
                        }
                        log.info("master and slaves" + masterSlaves);
                        jedis.disconnect();
                        break outer;
                    }
                } catch (JedisConnectionException e) {
                    log.warning("Cannot connect to sentinel running @ " + hap
                            + ". Trying next one.");
                }
            }

            try {
                log.severe("All sentinels down, cannot determine where is "
                        + masterName + " master is running... sleeping 1000ms.");
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        log.info("Redis master running at " + master
                + ", starting Sentinel listeners...");

        for (String sentinel : sentinels) {
            final HostAndPort hap = toHostAndPort(Arrays.asList(sentinel
                    .split(":")));
            JedisSentinelSlavePool.MasterListener masterListener = new JedisSentinelSlavePool.MasterListener(masterName,
                    hap.getHost(), hap.getPort());
            masterListeners.add(masterListener);
            masterListener.start();
        }
        return masterSlaves;
    }

    private HostAndPort toHostAndPort(List<String> getMasterAddrByNameResult) {
        String host = getMasterAddrByNameResult.get(0);
        int port = Integer.parseInt(getMasterAddrByNameResult.get(1));

        return new HostAndPort(host, port);
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

        protected String masterName;
        protected String host;
        protected int port;
        protected long subscribeRetryWaitTimeMillis = 5000;
        protected Jedis j;
        protected AtomicBoolean running = new AtomicBoolean(false);

        protected MasterListener() {
        }

        public MasterListener(String masterName, String host, int port) {
            this.masterName = masterName;
            this.host = host;
            this.port = port;
        }

        public MasterListener(String masterName, String host, int port,
                              long subscribeRetryWaitTimeMillis) {
            this(masterName, host, port);
            this.subscribeRetryWaitTimeMillis = subscribeRetryWaitTimeMillis;
        }

        public void run() {

            running.set(true);

            while (running.get()) {

                j = new Jedis(host, port);

                try {
                    j.subscribe(new JedisSentinelSlavePool.JedisPubSubAdapter() {
                        @Override
                        public void onMessage(String channel, String message) {
                            log.info("channel:" + channel);
                            log.info("message：" + message);
                            log.info("Sentinel " + host + ":" + port + " published: " + message + ".");
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
                        log.severe("Lost connection to Sentinel at " + host
                                + ":" + port
                                + ". Sleeping 5000ms and retrying.");
                        try {
                            Thread.sleep(subscribeRetryWaitTimeMillis);
                        } catch (InterruptedException e1) {
                            e1.printStackTrace();
                        }
                    } else {
                        log.fine("Unsubscribing from Sentinel at " + host + ":"
                                + port);
                    }
                }
            }
        }

        public void shutdown() {
            try {
                log.fine("Shutting down listener on " + host + ":" + port);
                running.set(false);
                // This isn't good, the Jedis object is not thread safe
                j.disconnect();
            } catch (Exception e) {
                log.severe("Caught exception while shutting down: "
                        + e.getMessage());
            }
        }


        private void switchMaster(String message) {
            String[] switchMasterMsg = message.split(" ");

            if (switchMasterMsg.length > 3) {

                if (masterName.equals(switchMasterMsg[0])) {
                    initPool(toHostAndPort(Arrays.asList(
                            switchMasterMsg[3],
                            switchMasterMsg[4])));
                    removeSlavePool(toHostAndPort(Arrays.asList(switchMasterMsg[3], switchMasterMsg[4])));
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
                HostAndPort hostAndPort = new HostAndPort(split[2], Integer.parseInt(split[3]));
                addSlavePool(hostAndPort);
            } else {
                log.info("Invalid message received on Sentinel   on channel +slave ：" + message);
            }
        }


        private void addSdown(String message) {
            String[] split = message.split(" ");
            if (split.length > 2) {
                if (split[0].equals("slave")) {
                    HostAndPort hostAndPort = new HostAndPort(split[2], Integer.parseInt(split[3]));
                    log.info("slave:" + hostAndPort + "+sdown");
                    removeSlavePool(hostAndPort);
                } else {
                    log.info("not slave +sdown message");
                }
            } else {
                log.info("Invalid message received on Sentinel   on channel +sdown：" + message);
            }
        }


        public void subtractSdown(String message) {
            String[] split = message.split(" ");
            if (split.length > 2) {
                if (split[0].equals("slave")) {
                    HostAndPort hostAndPort = new HostAndPort(split[2], Integer.parseInt(split[3]));
                    log.info("slave:" + hostAndPort + "-sdown");
                    addSlavePool(hostAndPort);
                } else {
                    log.info("not slave -sdown message");
                }
            } else {
                log.info("Invalid message received on Sentinel   on channel -sdown：" + message);
            }
        }
    }


}

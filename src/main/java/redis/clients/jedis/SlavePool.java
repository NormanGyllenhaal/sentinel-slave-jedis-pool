package redis.clients.jedis;

import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.util.Pool;

public class SlavePool<T> extends Pool<T> {

    public SlavePool(GenericObjectPoolConfig poolConfig, PooledObjectFactory<T> factory) {
        super(poolConfig, factory);
    }
}

this is a sentinel jedis pool ,can read from slave ,write from master 
```java
 Jedis master = pool.getResource();
 Jedis slave = pool.getSlaveResource();
 msster.set("test","");
 slave.get("test")
```
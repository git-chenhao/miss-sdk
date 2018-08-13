package com.miss.redis;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.JSONSerializer;
import com.alibaba.fastjson.serializer.SerializeWriter;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * redis工具类
 *
 * @author chenhao
 * @version 1.0
 * @date 2018/8/13 上午10:47
 */
public class RedisUtil {

    private static Logger logger = LoggerFactory.getLogger(RedisUtil.class);

    @Autowired
    private RedisTemplate redisTemplate;

    /**
     * isExistsKey
     *
     * @param redisKey
     * @return
     */
    public boolean isExistsKey(String redisKey) {
        try {
            return redisTemplate.hasKey(redisKey);
        } catch (Exception e) {
            logger.error("isExistsKey error, key:{}", redisKey, e);
            return false;
        }
    }

    /**
     * set
     *
     * @param key
     * @param value
     * @param time
     * @param timeUnit
     */
    public void set(String key, Object value, int time, TimeUnit timeUnit) {
        try {
            if (!Objects.isNull(value)) {
                redisTemplate.opsForValue().set(key, value, time, timeUnit);
            }
        } catch (Exception e) {
            logger.error("set error,key:{},value:{}", key, JSON.toJSONString(value), e);
        }
    }


    /**
     * 从value类型获取value
     *
     * @param key
     * @param clazz
     * @return
     */
    public <T> T get(String key, Class<T> clazz) {
        try {
            String objectJson = (String) redisTemplate.opsForValue().get(key);
            if (StringUtils.isBlank(objectJson)) {
                return null;
            }
            return JSON.parseObject(objectJson, clazz);
        } catch (Exception e) {
            logger.error("get value error,key:{}", key, e);
        }
        return null;
    }

    /**
     * 分布式锁
     *
     * @param key
     * @param expireTime
     * @param timeUnit
     * @param iProcess
     * @param <T>
     * @return
     */
    public <T> T lock(final String key, final int expireTime, TimeUnit timeUnit, IProcess iProcess) {
        long now = System.currentTimeMillis();
        boolean hasLock = false;
        try {
            if (!this.setNx(key, now)) {
                Long value = this.get(key, Long.class);
                if (value != null) {
                    if ((now - value.longValue()) > timeUnit.toMillis(value)) {
                        logger.info("Occupy key long time,key:{}, use time:{}s，force release key!", key, value.longValue(), timeUnit.toSeconds(value));
                        this.delete(key);
                    }
                    throw new RuntimeException("Operation is too frequent. Please wait for a second try.");
                }
                return null;
            } else {
                hasLock = true;
                this.expire(key, expireTime, timeUnit);
                return iProcess.process();
            }
        } finally {
            if (hasLock) {
                Long value = this.get(key, Long.class);
                if (value != null && value.longValue() != now) {
                    logger.error("The value of the lock is not consistent with the setting time. now lock value:{},Previously set value:{}", value, now);
                }
                this.delete(key);
            }
        }
    }

    /**
     * expire
     *
     * @param key
     * @param expireTime
     * @param timeUnit
     */
    public void expire(String key, int expireTime, TimeUnit timeUnit) {
        try {
            redisTemplate.expire(key, expireTime, timeUnit);
        } catch (Exception e) {
            logger.error("expire error,key:{}", key);
        }
    }


    /**
     * delete
     *
     * @param key
     */
    public void delete(String key) {
        try {
            redisTemplate.delete(key);
        } catch (Exception e) {
            logger.error("delete error,key:{}", key, e);
        }
    }

    public Boolean setNx(final String key, final Object value) {
        return (Boolean) redisTemplate.execute((RedisCallback<Boolean>) connection -> {
            SerializeWriter out = new SerializeWriter();
            JSONSerializer serializer = new JSONSerializer(out);
            serializer.write(value);
            return connection.setNX(key.getBytes(), out.toBytes("utf-8"));
        });
    }
}

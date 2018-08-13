package com.miss.redis;

/**
 * @author chenhao
 */
public interface IProcess {

    /**
     * 回调
     * @param <T>
     * @return
     */
    <T> T process();
}

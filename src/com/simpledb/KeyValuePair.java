package com.simpledb;

import java.io.Serializable;

public class KeyValuePair<K, T> {

    private K key;
    private T value;

    public KeyValuePair(K key, T value){
        this.key = key;
        this.value = value;
    }

    public K getKey() {
        return key;
    }

    public void setKey(K key) {
        this.key = key;
    }

    public T getValue() {
        return value;
    }

    public void setValue(T value) {
        this.value = value;
    }

    @Override
    public String toString(){
        return String.format("{%s, %s}", key, value);
    }
}

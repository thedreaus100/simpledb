package com.simpledb;

import java.io.Serializable;

public class KeyValuePair<K> {

    private K key;
    private Serializable value;

    public KeyValuePair(K key, Serializable value){
        this.key = key;
        this.value = value;
    }

    public K getKey() {
        return key;
    }

    public void setKey(K key) {
        this.key = key;
    }

    public Serializable getValue() {
        return value;
    }

    public void setValue(Serializable value) {
        this.value = value;
    }

    @Override
    public String toString(){
        return String.format("{%s, %s}", key, value);
    }
}

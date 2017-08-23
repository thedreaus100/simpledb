package main.com.simpledb.index.src.main.com.simpledb;

public class KeyValuePair<K> {

    private K key;
    private Object value;

    public KeyValuePair(K key, Object value){
        this.key = key;
        this.value = value;
    }

    public K getKey() {
        return key;
    }

    public void setKey(K key) {
        this.key = key;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    @Override
    public String toString(){
        return String.format("{%s, %s}", key, value);
    }
}

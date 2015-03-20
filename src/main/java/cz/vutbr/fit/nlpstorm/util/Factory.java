package cz.vutbr.fit.nlpstorm.util;

public interface Factory<T> {
    public T create();
    public void destroy();
}
package br.com.viniciusxyz.ecommerce;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class GsonDeserializer<T> implements Deserializer<T> {

    public static final String TYPE_CONFIG = "br.com.viniciusxyz.GsonDeserializer.type";
    private final Gson gson = new GsonBuilder().create();
    private Class<T> type;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

        String typeName = String.valueOf(configs.get(TYPE_CONFIG));
        try {
            System.out.println("Tipo de deserialização: " + typeName);
            this.type = (Class<T>) Class.forName(typeName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            throw new RuntimeException("Type: " + typeName + " not exists in classpath.");
        }
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        return gson.fromJson(new String(data), type);
    }
}

package com.nazjara.tf_idf.model;

import java.io.*;

public class SerializationUtils {

    public static byte[] serialize(Object object) {
        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
             ObjectOutput objectOutput = new ObjectOutputStream(byteArrayOutputStream)) {
            objectOutput.writeObject(object);
            objectOutput.flush();

            return byteArrayOutputStream.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return new byte[]{};
    }

    public static Object deserialize(byte[] input) {
        try (ByteArrayInputStream inputStream = new ByteArrayInputStream(input);
             ObjectInput objectInput = new ObjectInputStream(inputStream)) {
            return objectInput.readObject();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }

        return null;
    }
}
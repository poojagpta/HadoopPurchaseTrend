package com.xyz.hadoop;

import java.io.File;
import java.io.IOException;
 
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
 
public class JsonParserExample {
 
    public static void main(String[] args) {
 
        try {
 
            JsonFactory jfactory = new JsonFactory();
 
            /*** READ JSON DATA FROM FILE ***/
            JsonParser jParser = jfactory
                    .createParser(new File("/home/hduser/dev/data/data.json"));
 
            // LOOP UNTIL WE READ END OF JSON DATA, INDICATED BY }
            while (jParser.nextToken() != JsonToken.END_OBJECT) {
 
                String fieldname = jParser.getCurrentName();
                if ("blogname".equals(fieldname)) {
                    // once we get the token name we are interested,
                    // move next to get its value
                    jParser.nextToken();
                    // read the value of blogname which is javahash.com
                    System.out.println(jParser.getText());
                }
                // reading the json -> subject: java
                if ("subject".equals(fieldname)) {
                    jParser.nextToken();
                    System.out.println(jParser.getText());
                }
                // reading the posts data which is an array
                if ("posts".equals(fieldname)) {
                    // current token is "[" begining of array. So we move next
                    jParser.nextToken();
                    // iterate through the array until token equal to "]"
                    while (jParser.nextToken() != JsonToken.END_ARRAY) {
                        // output the array data
                        System.out.println(jParser.getText());
                    }
                }
 
            }
            jParser.close();
 
        } catch (JsonGenerationException e) {
 
            e.printStackTrace();
 
        } catch (IOException e) {
 
            e.printStackTrace();
 
        }
 
    }
}
package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.client.methods.*;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class AstraDbRestService {
    private static final Logger logger = LogManager.getLogger(AstraDbRestService.class);
    private static String BASE_URL;
    private static String AUTH_TOKEN;

    static {
        Properties prop = new Properties();
        try (InputStream input = AstraDbRestService.class.getClassLoader().getResourceAsStream("config.properties")) {
            if (input == null) {
                logger.error("Sorry, unable to find config.properties");
            } else {
                prop.load(input);
                BASE_URL = "https://" + prop.getProperty("ASTRA_DB_ID") + "-" + prop.getProperty("ASTRA_DB_REGION") +
                        ".apps.astra.datastax.com/api/rest/v2/keyspaces/" + prop.getProperty("ASTRA_DB_KEYSPACE");
                AUTH_TOKEN = prop.getProperty("ASTRA_DB_APPLICATION_TOKEN");
            }
        } catch (IOException ex) {
            logger.error("IOException occurred", ex);
        }
    }

    public static void main(String[] args) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {

            // Create table
            String createTableJson = "{"
                    + "\"name\": \"rest_example_products\","
                    + "\"ifNotExists\": true,"
                    + "\"columnDefinitions\": ["
                    + "{\"name\": \"id\", \"typeDefinition\": \"uuid\", \"static\": false},"
                    + "{\"name\": \"productname\", \"typeDefinition\": \"text\", \"static\": false},"
                    + "{\"name\": \"description\", \"typeDefinition\": \"text\", \"static\": false},"
                    + "{\"name\": \"price\", \"typeDefinition\": \"decimal\", \"static\": false},"
                    + "{\"name\": \"created\", \"typeDefinition\": \"timestamp\", \"static\": false}"
                    + "],"
                    + "\"primaryKey\": {\"partitionKey\": [\"id\"]},"
                    + "\"tableOptions\": {\"defaultTimeToLive\": 0}"
                    + "}";

            HttpPost createTablePost = new HttpPost(BASE_URL + "/tables");
            createTablePost.setHeader("Content-Type", "application/json");
            createTablePost.setHeader("x-cassandra-token", AUTH_TOKEN);
            createTablePost.setEntity(new StringEntity(createTableJson));

            try (CloseableHttpResponse response = httpClient.execute(createTablePost)) {
                logger.info("Create table response: " + EntityUtils.toString(response.getEntity()));
            }

            // Insert record
            String insertRecordJson = "{"
                    + "\"id\":\"e9b6c02d-0604-4bab-a3ea-6a7984654631\","
                    + "\"productname\":\"Heavy Lift Arms\","
                    + "\"description\":\"Heavy lift arms capable of lifting 1,250 lbs of weight per arm. Sold as a set.\","
                    + "\"price\":1500.00,"
                    + "\"created\":\"2024-06-03T10:00:00Z\""
                    + "}";

            HttpPost insertRecordPost = new HttpPost(BASE_URL + "/rest_example_products");
            insertRecordPost.setHeader("Content-Type", "application/json");
            insertRecordPost.setHeader("x-cassandra-token", AUTH_TOKEN);
            insertRecordPost.setEntity(new StringEntity(insertRecordJson));

            try (CloseableHttpResponse response = httpClient.execute(insertRecordPost)) {
                logger.info("Insert record response: " + EntityUtils.toString(response.getEntity()));
            }

        }
    }
}

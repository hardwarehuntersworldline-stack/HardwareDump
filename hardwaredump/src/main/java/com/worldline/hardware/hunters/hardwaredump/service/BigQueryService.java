package com.worldline.hardware.hunters.hardwaredump.service;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;
import org.springframework.stereotype.Service;

@Service
public class BigQueryService {

    private final BigQuery bigQuery;

    public BigQueryService() {
        // Automatically uses Application Default Credentials
        this.bigQuery = BigQueryOptions.getDefaultInstance().getService();
    }

    /**
     * Run a simple query against BigQuery and return the results as a formatted String.
     *
     * @param dataset the dataset name
     * @param table   the table name
     * @return query results as string
     */
    public String runSimpleQuery(String dataset, String table) {
        String query = String.format("SELECT * FROM `%s.%s` LIMIT 10", dataset, table);
        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();

        try {
            TableResult result = bigQuery.query(queryConfig);
            StringBuilder sb = new StringBuilder();

            for (FieldValueList row : result.iterateAll()) {
                for (FieldValue fieldValue : row) {
                    sb.append(fieldValue.toString()).append("\t");
                }
                sb.append("\n");
            }
            return sb.toString();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("BigQuery query was interrupted", e);
        } catch (Exception e) {
            throw new RuntimeException("BigQuery query failed", e);
        }
    }
}

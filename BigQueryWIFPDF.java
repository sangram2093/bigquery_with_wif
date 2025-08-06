package org.example;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.*;
import com.itextpdf.kernel.colors.ColorConstants;
import com.itextpdf.kernel.pdf.*;
import com.itextpdf.layout.*;
import com.itextpdf.layout.element.*;
import com.itextpdf.layout.property.*;
import com.itextpdf.layout.borders.*;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class BigQueryWIFPDF {

    public static void main(String[] args) throws IOException, InterruptedException {

        // ====== CONFIGURATION ======
        String wifHome = System.getenv("WIF_HOME");
        String wifTokenFilename = wifHome + "/wif_token.txt";
        String projectName = System.getenv("PROJECT_NAME");
        String location = System.getenv("LOCATION"); // e.g., "us"
        String outputPdfPath = "bigquery_results.pdf";

        // ====== AUTHENTICATION ======
        String accessTokenString = Files.readString(Paths.get(wifTokenFilename)).trim();
        AccessToken accessToken = new AccessToken(accessTokenString, null);
        GoogleCredentials credentials = GoogleCredentials.create(accessToken)
                .createScoped(Collections.singletonList("https://www.googleapis.com/auth/cloud-platform"));

        BigQuery bigquery = BigQueryOptions.newBuilder()
                .setCredentials(credentials)
                .setProjectId(projectName)
                .setLocation(location)
                .build()
                .getService();

        // ====== QUERY ======
        String selectQuery =
                "SELECT exchange, symbol, trade_date, price, volume " +
                "FROM `your_project.your_dataset.your_table` " +
                "ORDER BY exchange, symbol LIMIT 50";

        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(selectQuery)
                .setUseLegacySql(false)
                .build();

        JobId jobId = JobId.of(projectName, "WIF_QUERY_JOB_" + System.currentTimeMillis());
        Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build()).waitFor();

        if (queryJob == null) {
            throw new RuntimeException("Job no longer exists");
        } else if (queryJob.getStatus().getError() != null) {
            throw new RuntimeException(queryJob.getStatus().getError().toString());
        }

        TableResult results = queryJob.getQueryResults();

        // ====== GROUP DATA BY EXCHANGE ======
        List<String> columnNames = new ArrayList<>();
        for (Field field : results.getSchema().getFields()) {
            columnNames.add(field.getName());
        }

        Map<String, List<List<String>>> groupedData = new LinkedHashMap<>();

        for (FieldValueList row : results.iterateAll()) {
            String exchange = row.get("exchange").isNull() ? "UNKNOWN" : row.get("exchange").getStringValue();

            List<String> rowData = new ArrayList<>();
            for (String colName : columnNames) {
                rowData.add(row.get(colName).isNull() ? "" : row.get(colName).getStringValue());
            }

            groupedData.computeIfAbsent(exchange, k -> new ArrayList<>()).add(rowData);
        }

        // ====== CREATE PDF ======
        createPdf(outputPdfPath, groupedData, columnNames);

        System.out.println("PDF created successfully: " + outputPdfPath);
    }

    private static void createPdf(String outputPath, Map<String, List<List<String>>> groupedData,
                                  List<String> columnNames) throws FileNotFoundException {

        PdfWriter writer = new PdfWriter(outputPath);
        PdfDocument pdfDoc = new PdfDocument(writer);
        Document document = new Document(pdfDoc, PageSize.A4.rotate()); // Landscape mode for wide tables

        for (Map.Entry<String, List<List<String>>> entry : groupedData.entrySet()) {
            String exchange = entry.getKey();
            List<List<String>> rows = entry.getValue();

            // ====== Page Title ======
            document.add(new Paragraph("Exchange: " + exchange)
                    .setFontSize(14)
                    .setBold()
                    .setMarginBottom(10));

            // ====== Table ======
            Table table = new Table(UnitValue.createPercentArray(columnNames.size()))
                    .useAllAvailableWidth();

            // ====== Header Row ======
            for (String colName : columnNames) {
                table.addHeaderCell(new Cell()
                        .add(new Paragraph(colName))
                        .setBackgroundColor(ColorConstants.LIGHT_GRAY)
                        .setBold()
                        .setTextAlignment(TextAlignment.CENTER)
                        .setVerticalAlignment(VerticalAlignment.MIDDLE)
                        .setPadding(5));
            }

            // ====== Data Rows ======
            boolean alternate = false;
            for (List<String> row : rows) {
                for (String value : row) {
                    Cell cell = new Cell().add(new Paragraph(value))
                            .setTextAlignment(TextAlignment.CENTER)
                            .setVerticalAlignment(VerticalAlignment.MIDDLE)
                            .setPadding(5)
                            .setBorder(new SolidBorder(0.5f));
                    if (alternate) {
                        cell.setBackgroundColor(ColorConstants.WHITE);
                    } else {
                        cell.setBackgroundColor(ColorConstants.LIGHT_GRAY);
                    }
                    table.addCell(cell);
                }
                alternate = !alternate;
            }

            document.add(table);
            document.add(new AreaBreak(AreaBreakType.NEXT_PAGE)); // Page break for next exchange
        }

        document.close();
    }
}

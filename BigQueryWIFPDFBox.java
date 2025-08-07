import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.*;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.PDPageContentStream;
import org.apache.pdfbox.pdmodel.PDPageContentStream.AppendMode;
import org.apache.pdfbox.pdmodel.common.PDRectangle;
import org.apache.pdfbox.pdmodel.font.PDType1Font;
import org.apache.pdfbox.pdmodel.font.PDFont;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class BigQueryPdfExporter {

    public static void main(String[] args) throws Exception {
        String wifHome = System.getenv("WIF_HOME");
        String wifTokenFilename = wifHome + "/wif_token.txt";
        String projectName = System.getenv("PROJECT_NAME");
        String location = System.getenv("LOCATION");

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

        String query = "SELECT exchange, client_name, order_id, price, quantity, symbol, timestamp, status, broker, region, strategy, notes " +
                       "FROM `your_project.your_dataset.your_table` LIMIT 100";
        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).setUseLegacySql(false).build();
        TableResult result = bigquery.query(queryConfig);

        List<String> columnNames = new ArrayList<>();
        for (Field field : result.getSchema().getFields()) {
            columnNames.add(field.getName());
        }

        Map<String, List<List<String>>> groupedData = new TreeMap<>();
        for (FieldValueList row : result.iterateAll()) {
            String key = row.get("exchange").getStringValue();
            groupedData.putIfAbsent(key, new ArrayList<>());

            List<String> rowData = new ArrayList<>();
            for (String col : columnNames) {
                rowData.add(row.get(col).isNull() ? "" : row.get(col).getValue().toString());
            }
            groupedData.get(key).add(rowData);
        }

        generatePdfWithWrapping(groupedData, columnNames, "BigQuery_Output.pdf");
        System.out.println("PDF generated successfully.");
    }

    private static void generatePdfWithWrapping(Map<String, List<List<String>>> dataByExchange,
                                                List<String> columnNames, String outputFile) throws IOException {

        PDFont headerFont = PDType1Font.HELVETICA_BOLD;
        PDFont cellFont = PDType1Font.HELVETICA;
        float headerFontSize = 7;
        float cellFontSize = 6.5f;
        float padding = 5f;

        PDDocument document = new PDDocument();
        int pageCount = 0;

        for (Map.Entry<String, List<List<String>>> entry : dataByExchange.entrySet()) {
            String exchange = entry.getKey();
            List<List<String>> rows = entry.getValue();

            PDPage page = new PDPage(new PDRectangle(PDRectangle.A4.getHeight(), PDRectangle.A4.getWidth())); // Landscape
            document.addPage(page);
            pageCount++;

            PDPageContentStream cs = new PDPageContentStream(document, page, AppendMode.APPEND, true);
            PDRectangle mediaBox = page.getMediaBox();

            float margin = 40;
            float yStart = mediaBox.getHeight() - 40;
            float yPosition = yStart;
            float rowHeight = 25;

            // Estimate column widths based on max content
            Map<Integer, Float> colWidths = new HashMap<>();
            for (int i = 0; i < columnNames.size(); i++) {
                float maxWidth = getTextWidth(columnNames.get(i), headerFont, headerFontSize);
                for (List<String> row : rows) {
                    if (i < row.size()) {
                        float w = getTextWidth(row.get(i), cellFont, cellFontSize);
                        if (w > maxWidth) maxWidth = w;
                    }
                }
                colWidths.put(i, maxWidth + 2 * padding);
            }

            // Draw top-left header
            cs.beginText();
            cs.setFont(headerFont, 9);
            cs.newLineAtOffset(margin, mediaBox.getHeight() - 25);
            cs.showText("Exchange: " + exchange);
            cs.endText();

            // Draw center header
            String centerHeader = "BigQuery Export - " + exchange;
            float centerTextWidth = getTextWidth(centerHeader, headerFont, 9);
            cs.beginText();
            cs.setFont(headerFont, 9);
            cs.newLineAtOffset((mediaBox.getWidth() - centerTextWidth) / 2, mediaBox.getHeight() - 25);
            cs.showText(centerHeader);
            cs.endText();

            // Draw footer
            String footer = "Page " + pageCount;
            float footerWidth = getTextWidth(footer, cellFont, 8);
            cs.beginText();
            cs.setFont(cellFont, 8);
            cs.newLineAtOffset(mediaBox.getWidth() - footerWidth - 40, 20);
            cs.showText(footer);
            cs.endText();

            // Draw table header
            float xPosition = margin;
            yPosition -= rowHeight;
            for (int i = 0; i < columnNames.size(); i++) {
                float width = colWidths.get(i);
                drawWrappedCell(cs, columnNames.get(i), xPosition, yPosition, width, rowHeight, headerFont, headerFontSize, padding);
                xPosition += width;
            }

            // Draw data rows
            yPosition -= rowHeight;
            for (List<String> row : rows) {
                xPosition = margin;
                float maxCellHeight = rowHeight;

                // Pre-calculate height for wrapped content
                for (int i = 0; i < columnNames.size(); i++) {
                    String text = (i < row.size()) ? row.get(i) : "";
                    List<String> lines = wrapText(text, cellFont, cellFontSize, colWidths.get(i) - 2 * padding);
                    float height = lines.size() * (cellFontSize + 2) + 2 * padding;
                    if (height > maxCellHeight) maxCellHeight = height;
                }

                for (int i = 0; i < columnNames.size(); i++) {
                    String text = (i < row.size()) ? row.get(i) : "";
                    drawWrappedCell(cs, text, xPosition, yPosition, colWidths.get(i), maxCellHeight, cellFont, cellFontSize, padding);
                    xPosition += colWidths.get(i);
                }

                yPosition -= maxCellHeight;
                if (yPosition <= 60) break; // stop if out of space
            }

            cs.close();
        }

        document.save(new File(outputFile));
        document.close();
    }

    private static void drawWrappedCell(PDPageContentStream cs, String text, float x, float y, float width,
                                        float height, PDFont font, float fontSize, float padding) throws IOException {
        cs.setLineWidth(0.5f);
        cs.addRect(x, y, width, height);
        cs.stroke();

        if (text == null || text.trim().isEmpty()) return;

        List<String> lines = wrapText(text, font, fontSize, width - 2 * padding);
        float lineHeight = fontSize + 2;
        float startY = y + height - padding - fontSize;

        for (String line : lines) {
            if (startY < y + padding) break;

            cs.beginText();
            cs.setFont(font, fontSize);
            cs.newLineAtOffset(x + padding, startY);
            cs.showText(line);
            cs.endText();

            startY -= lineHeight;
        }
    }

    private static List<String> wrapText(String text, PDFont font, float fontSize, float maxWidth) throws IOException {
        List<String> lines = new ArrayList<>();
        String[] words = text.split("\\s+");
        StringBuilder line = new StringBuilder();

        for (String word : words) {
            String temp = line.length() == 0 ? word : line + " " + word;
            float width = font.getStringWidth(temp) / 1000 * fontSize;
            if (width > maxWidth) {
                if (line.length() > 0) lines.add(line.toString());
                line = new StringBuilder(word);
            } else {
                line = new StringBuilder(temp);
            }
        }

        if (line.length() > 0) lines.add(line.toString());
        return lines;
    }

    private static float getTextWidth(String text, PDFont font, float fontSize) throws IOException {
        return font.getStringWidth(text) / 1000 * fontSize;
    }
}

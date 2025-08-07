import org.yaml.snakeyaml.Yaml;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class ColumnConfigLoader {
    public static Map<String, Float> loadColumnWidths() {
        Yaml yaml = new Yaml();
        try (InputStream in = ColumnConfigLoader.class.getClassLoader().getResourceAsStream("config.yaml")) {
            Map<String, Object> obj = yaml.load(in);
            Map<String, Object> rawWidths = (Map<String, Object>) obj.get("columnWidths");

            Map<String, Float> widths = new HashMap<>();
            for (Map.Entry<String, Object> entry : rawWidths.entrySet()) {
                widths.put(entry.getKey(), Float.parseFloat(entry.getValue().toString()));
            }

            return widths;
        } catch (Exception e) {
            throw new RuntimeException("Failed to load column widths from config.yaml", e);
        }
    }
}

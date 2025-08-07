import org.yaml.snakeyaml.Yaml;
import java.io.InputStream;
import java.util.Map;

public class ColumnConfigLoader {
    public static Map<String, Float> loadColumnWidths() {
        Yaml yaml = new Yaml();
        try (InputStream in = ColumnConfigLoader.class.getClassLoader().getResourceAsStream("config.yaml")) {
            Map<String, Object> obj = yaml.load(in);
            return (Map<String, Float>) obj.get("columnWidths");
        } catch (Exception e) {
            throw new RuntimeException("Failed to load config.yaml", e);
        }
    }
}

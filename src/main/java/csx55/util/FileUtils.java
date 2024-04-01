package csx55.util;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class FileUtils {
    public static Path createDirectoryIfNotExists(Path filePath) {
        Path path = null;
        try {
            path = Files.createDirectories(filePath.getParent());
        } catch (
                IOException e) {
            throw new RuntimeException(e);
        }
        return path;
    }
}

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


    public static String removeFileExtension(String fileName) {
        // Find the last index of '.'
        int dotIndex = fileName.lastIndexOf('.');
        // If there isn't any '.' or it's the first character, return the whole name
        if (dotIndex <= 0) {
            return fileName;
        }
        // Otherwise, return the substring up to the last '.'
        return fileName.substring(0, dotIndex);
    }
}

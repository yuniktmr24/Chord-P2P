package csx55.util;

import csx55.config.ChordConfig;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

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

    public static void printFileNamesWithHashCode(String fileStorageDirectory) {
        Path dir = Paths.get(fileStorageDirectory);
        try (Stream<Path> stream = Files.list(dir)) {
            stream.forEach(path -> {
                String fileName = path.getFileName().toString();
                long key = fileName.hashCode();
                if (ChordConfig.DEBUG_MODE) {
                    key = Long.parseLong(removeFileExtension(fileName));
                }
                System.out.println(fileName + " -> HashCode: " + key);
            });
        } catch (IOException e) {
            System.out.println("Error reading directory");
            e.printStackTrace();
        }
    }
}

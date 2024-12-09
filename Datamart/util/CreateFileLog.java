package util;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public class CreateFileLog {
    public static String createFIleLog(List<String> logs){
        String filePath = "logs.txt";
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath))) {
            for (String log : logs) {
                writer.write(log);
                writer.newLine();
            }
        } catch (IOException e) {
            return null;
        }
        return filePath;
    }
}

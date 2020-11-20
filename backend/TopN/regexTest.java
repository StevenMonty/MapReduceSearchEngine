import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class regexTest {

    public static void main(String[] args) throws FileNotFoundException {

        Matcher match;
        String line;
        Pattern regex = Pattern.compile("[a-zA-Z]+");

        File f = new File("/Users/StevenMontalbano/Programs/cs1660/FinalProject/frontend/src/main/resources/assets/Shakespeare/poetry/loverscomplaint");
        Scanner scan = new Scanner(f);

        while(scan.hasNextLine()) {
            line = scan.nextLine();
            match = regex.matcher(line);

            System.out.println("LINE: " + line);
            System.out.println("Parsed Words:");
            while(match.find())
                System.out.println(match.group().toLowerCase());
        }

    }
}

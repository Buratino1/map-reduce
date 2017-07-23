import java.io.*;
import java.util.Random;

public class GenData {
    public static void main(String[] args) throws IOException {
        final int posTotal = 6000;
        final int idsTotal = 100000;

        Random rand = new Random();
        String rowType="";
        int amount ;


        File outFile = new File("c:/tmp/outfile.txt") ;
        BufferedWriter fw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outFile, true), "windows-1252"));

        for(int posId=1; posId<=posTotal; posId++){
            fw.write(posId+";0;Original;100\r\n") ;
            amount = 100 ;
            for(int idVal=1; idVal<=idsTotal; idVal++){
                if (rand.nextInt(2) > 0) {
                    rowType = "adjusted" ;
                } else {
                    rowType = "original" ;
                }
                amount = amount + rand.nextInt(20000);
                String str = posId+";"+idVal+";" +rowType+";"+amount+ "\r\n" ;
                fw.write(str) ;
            }

        }
        fw.close();
    }
}

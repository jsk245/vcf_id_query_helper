import java.nio.file.Path;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.BufferedReader;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.nio.charset.StandardCharsets;
import java.io.FileOutputStream;
import java.lang.ProcessBuilder;
import java.lang.Process;
import java.io.PrintStream;
import java.util.UUID;
import java.util.HashSet;

public class DB {
    private static void createDB(String filename) {
        Path path = Paths.get(filename);

        String outFileName = filename.substring(0, filename.indexOf(".vcf")) + "MAP.vcf";
	    String outFileSortedName = outFileName.substring(0, outFileName.indexOf(".vcf")) + ".SORTED.vcf";
        //System.out.println(outFileSortedName);
        try(
        InputStream is = Files.newInputStream(path);
        GZIPInputStream gis = new GZIPInputStream(is);
        InputStreamReader isReader = new InputStreamReader(gis, StandardCharsets.UTF_8);
        BufferedReader br = new BufferedReader(isReader); 

        OutputStream out = new FileOutputStream(outFileName);
        PrintStream outPrintStream = new PrintStream(out);
        OutputStream outSorted = new FileOutputStream(outFileSortedName);
        PrintStream outSortedPrintStream = new PrintStream(outSorted);
        ) {
            String line;
            String[] lineSlice = new String[3];
            lineSlice[0] = "1";
            String[] lineArray;
            String location;
	        String id;

            outSortedPrintStream.println("##fileformat=VCFv4.2");
            outSortedPrintStream.println("#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO");

            while ((line = br.readLine()) != null) {
                if (line.charAt(0) != '#') {
                    lineArray = line.trim().split("\t");
                    if (!lineArray[2].equals(".")) {
                        location = lineArray[0] + ":" + lineArray[1];
			            id = lineArray[2];

                        lineSlice[2] = location;

                        while (id.indexOf(";") != -1) {
				            lineSlice[1] = "" + (id.substring(0, id.indexOf(";")).hashCode() & 0xFFFFFFF);
                        	outPrintStream.print(String.join("\t", lineSlice));
                            outPrintStream.print("\tA\t.\t.\t.\t.");
                            outPrintStream.println();
                            id = id.substring(id.indexOf(";")+1);
			            }
                        lineSlice[1] = "" + (id.hashCode() & 0xFFFFFFF);
                        outPrintStream.print(String.join("\t", lineSlice));
                        outPrintStream.print("\tA\t.\t.\t.\t.");
                        outPrintStream.println();
                    }
                }
	        }
            sortFile(outFileName, outSortedPrintStream);

            remove(outFileName);

            bgzip(outFileSortedName);

            tabixIndex(outFileSortedName + ".gz");
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private static void query(String idList, String fileName) {
        String tempFileName = UUID.randomUUID().toString() + ".txt";

        HashSet<String> idSet = new HashSet<>();
        Path path = Paths.get(idList);
        try (
        InputStream is = Files.newInputStream(path);
        InputStreamReader isReader = new InputStreamReader(is);
        BufferedReader br = new BufferedReader(isReader);
        OutputStream out = new FileOutputStream(tempFileName);
        PrintStream outPrintStream = new PrintStream(out);

        OutputStream out2 = new FileOutputStream(tempFileName + "2");
        PrintStream outPrintStream2 = new PrintStream(out2);
        ) {
            String line;
            outPrintStream.print("#CHROM\tPOS\n");
            outPrintStream2.print("#CHROM\tPOS\n");

            while ((line = br.readLine()) != null) {
                idSet.add(line);
                outPrintStream.print("1\t");
                outPrintStream.print("" + (line.hashCode() & 0xFFFFFFF));
                outPrintStream.println();
            }

            tabixQuery(tempFileName, fileName.substring(0, fileName.indexOf(".vcf")) + "MAP.SORTED.vcf.gz", outPrintStream2);
            remove(tempFileName);

            tabixQuery(tempFileName + "2", fileName, idSet);
            remove(tempFileName + "2");
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private static void sortFile(String fileName, PrintStream outSortedPrintStream) {
        try {
            String[] command = {"sort", "-k2,2n", fileName};
            String line;
            ProcessBuilder builder = new ProcessBuilder(command);
            Process p = builder.start();
            BufferedReader outputReader = new BufferedReader(new InputStreamReader(p.getInputStream()));
            while ((line = outputReader.readLine()) != null) {
                outSortedPrintStream.print(line);
                outSortedPrintStream.println();
            }
            p.waitFor();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
    
    private static void remove(String fileName) {
        try {
            String[] cmd = {"rm", fileName};
            ProcessBuilder builder = new ProcessBuilder(cmd);
            Process p = builder.start();
            p.waitFor();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private static void bgzip(String fileName) {
        try {
            String[] cmd = {"bgzip", fileName};
            ProcessBuilder builder = new ProcessBuilder(cmd);
            Process p = builder.start();
            p.waitFor();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private static void tabixIndex(String fileName) {
        try {
            String[] cmd = {"tabix", "-C", fileName};
            ProcessBuilder builder = new ProcessBuilder(cmd);
            Process p = builder.start();
            p.waitFor();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private static void tabixQuery(String idList, String fileName, PrintStream outSortedPrintStream) {
        try {
            String[] command = {"tabix", "-R", idList, fileName};
            String line;
            ProcessBuilder builder = new ProcessBuilder(command);
            Process p = builder.start();
            BufferedReader outputReader = new BufferedReader(new InputStreamReader(p.getInputStream()));
            while ((line = outputReader.readLine()) != null) {
                line = line.substring(line.indexOf("\t") + 1);
                line = line.substring(line.indexOf("\t") + 1);
                line = line.substring(0, line.indexOf("\t"));
                
                outSortedPrintStream.print(line.substring(0, line.indexOf(":")));
                outSortedPrintStream.print("\t");
                line = line.substring(line.indexOf(":") + 1);
                outSortedPrintStream.print(line);
                outSortedPrintStream.println();
            }
            p.waitFor();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
    
    private static void tabixQuery(String idList, String fileName, HashSet<String> idSet) {
        try {
            String[] command = {"tabix", "-R", idList, fileName};
            String line;
            String id;
            ProcessBuilder builder = new ProcessBuilder(command);
            Process p = builder.start();
            BufferedReader outputReader = new BufferedReader(new InputStreamReader(p.getInputStream()));
            while ((line = outputReader.readLine()) != null) {
                id = line.substring(line.indexOf("\t") + 1);
                id = id.substring(id.indexOf("\t") + 1);
                id = id.substring(0, id.indexOf("\t"));

                while (true) {
                    if (id.indexOf(";") != -1) {
                        if (idSet.contains(id.substring(0, id.indexOf(";")))) {
                            System.out.println(line);
                            break;
                        } else {
                            id = id.substring(id.indexOf(";") + 1);
                        }
                    } else {
                        if (idSet.contains(id)) {
                            System.out.println(line);
                        }
                        break;
                    }
                }
            }
            p.waitFor();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public static void main(String[] args) {
        if (args[0].equals("createDB")) {
            createDB(args[1]);
        } else if (args[0].equals("query")) {
            query(args[1], args[2]);
        }
    }
}

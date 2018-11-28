package fr.kriszt.theo;

import java.io.*;
import java.util.*;

public class Main {

    private static String queriesPath, dataPath, outPath, jarPath;
    private static File jar, outDir, inData, inQueries;
    private static int times = 1;
    private static GroupEnum group;
    private static boolean verbose;

    public static void main(String[] args) {

        /*
         -queries "res/queries" -data "res/dataset" -output "out/wrapper" -jar "res/jar/us.jar" -group "us" -times 1000
         */

        long time =  System.currentTimeMillis();
        readParams(args);

        startJar(jar, inData, inQueries, outDir);

        try {
            extractData();
        } catch (IOException e) {
            e.printStackTrace();
        }

        time = System.currentTimeMillis() - time;
        time /= 1000;

        System.out.println("-----------------------------------------");
        System.out.println("Total execution time : " + time + "s");


    }

    private static void extractData() throws IOException {

        if (group == GroupEnum.US) {
            extractOurData();
        } else extractTheirData();

    }


    private static void extractOurData() throws IOException {

        ArrayList<Float> queriesReadingTimes = new ArrayList<>();
        ArrayList<Float> datasetReadingTimes = new ArrayList<>();
        ArrayList<Float> processingTimes = new ArrayList<>();
        ArrayList<Float> executionTimes = new ArrayList<>();


//        System.out.println("Output dir :: " + outDir);
//        System.out.println("Output path :: " + outPath);

        ArrayList<File> outDirs = new ArrayList<>();
        for (int i = 0; i < times; i++){
            outDirs.add(new File( outDir + "_" + i ));
        }

        for (File output : outDirs) {
            if (output.exists()){
                File workload = new File(output + "/workload_time.csv");

                BufferedReader br = new BufferedReader(new FileReader(workload));

                String line;
                while ((line = br.readLine()) != null) {
//                    System.out.println(line);
                    if (line.startsWith("Lecture des requ")){
                        queriesReadingTimes.add( readMsFromLine(line) );
                    }else if (line.startsWith("Lecture du jeu de donn")){
                        datasetReadingTimes.add(readMsFromLine(line));
                    }else if (line.startsWith("Traitement des requ")){
                        processingTimes.add(readMsFromLine(line));
                    }else if (line.startsWith("Temps total")){
                        executionTimes.add(readMsFromLine(line));
                    }
                }

            }


        }

//        System.out.println("Taille des données à extraire : ");
//        System.out.println(executionTimes.size());

        writeExecutionStats(queriesReadingTimes, datasetReadingTimes, processingTimes, executionTimes);

    }

    private static void writeExecutionStats(List<Float> queriesReadingTimes, List<Float> datasetReadingTimes, List<Float> processingTimes, List<Float> executionTimes) throws IOException {
        Collections.sort(queriesReadingTimes);
        Collections.sort(datasetReadingTimes);
        Collections.sort(processingTimes);
        Collections.sort(executionTimes);

        ArrayList<List<Float>> lists = new ArrayList<>();
        lists.add(queriesReadingTimes);
        lists.add(datasetReadingTimes);
        lists.add(processingTimes);
        lists.add(executionTimes);

        final int TRUST_INTERVAL = 90;
        System.out.println("TRUST INTERVAL : " + TRUST_INTERVAL);
        int TRUST_LOW =  ((100 - TRUST_INTERVAL) /2);   // 5
        System.out.println("TRUST LOW (base 100) : " + TRUST_LOW);
        TRUST_LOW = Math.round( (TRUST_LOW * ((float)times) / 100));   // 5
        System.out.println("TRUST LOW (base "+times+") : " + TRUST_LOW);
        int TRUST_HIGH = times - TRUST_LOW;



        System.out.println("Valeurs a conserver sur une liste de " + times + " elements : entre " + TRUST_LOW + " et " + TRUST_HIGH);




        // Eliminer les valeurs en dehors de l'intervalle de confiance
        if( times >= 10 ){
            queriesReadingTimes = queriesReadingTimes.subList(TRUST_LOW, TRUST_HIGH);
            datasetReadingTimes = datasetReadingTimes.subList(TRUST_LOW, TRUST_HIGH);
            processingTimes = processingTimes.subList(TRUST_LOW, TRUST_HIGH);
            executionTimes = executionTimes.subList(TRUST_LOW, TRUST_HIGH);
        }



        // Calcul des quartiles
        int q1Index = (int) Math.ceil(executionTimes.size() * 0.25);
        int q2Index = (int) Math.round(executionTimes.size() * 0.5);
        int q3Index = (int) Math.floor(executionTimes.size() * 0.75);

        System.out.println("Index Q1 : "+ q1Index + ", index mediane : " + q2Index + ", index q3 : " + q3Index);


        float queriesReadingMin =      queriesReadingTimes.get(0);
        float queriesReadingMax =      queriesReadingTimes.get(queriesReadingTimes.size()-1);
        float datasetReadingMin =      datasetReadingTimes.get(0);
        float datasetReadingMax =      datasetReadingTimes.get(queriesReadingTimes.size()-1);
        float processingMin =          processingTimes.get(0);
        float processingMax =          processingTimes.get(queriesReadingTimes.size()-1);
        float executionMin =           executionTimes.get(0);
        float executionMax =           executionTimes.get(queriesReadingTimes.size()-1);

        float queriesReadingq1 =      queriesReadingTimes.get(q1Index);
        float datasetReadingq1 =    datasetReadingTimes.get(q1Index);
        float processingq1 =   processingTimes.get(q1Index);
        float executionq1 =      executionTimes.get(q1Index);

        float queriesReadingq2 =      queriesReadingTimes.get(q2Index);
        float datasetReadingq2 =    datasetReadingTimes.get(q2Index);
        float processingq2 =   processingTimes.get(q2Index);
        float executionq2 =      executionTimes.get(q2Index);

        float queriesReadingq3 =      queriesReadingTimes.get(q3Index);
        float datasetReadingq3 =    datasetReadingTimes.get(q3Index);
        float processingq3 =   processingTimes.get(q3Index);
        float executionq3 =      executionTimes.get(q3Index);

        System.out.println("Nouvelle taille des listes (final) : " + processingTimes.size());

        Map<String, Float> stats = new TreeMap<>();
        stats.put("1.1-Lecture des requetes Min", queriesReadingMin);
        stats.put("1.2-Lecture des requetes Q1", queriesReadingq1);
        stats.put("1.3-Lecture des requetes Q2", queriesReadingq2);
        stats.put("1.4-Lecture des requetes Q3", queriesReadingq3);
        stats.put("1.5-Lecture des requetes Max", queriesReadingMax);

        stats.put("2.1-Lecture des donnees Min", datasetReadingMin);
        stats.put("2.2-Lecture des donnees Q1", datasetReadingq1);
        stats.put("2.3-Lecture des donnees Q2", datasetReadingq2);
        stats.put("2.4-Lecture des donnees Q3", datasetReadingq3);
        stats.put("2.5-Lecture des donnees Max", datasetReadingMax);

        stats.put("3.1-Traitement des requetes Min", processingMin);
        stats.put("3.2-Traitement des requetes Q1", processingq1);
        stats.put("3.3-Traitement des requetes Q2", processingq2);
        stats.put("3.4-Traitement des requetes Q3", processingq3);
        stats.put("3.5-Traitement des requetes Max", processingMax);

        stats.put("4.1-Total Min", executionMin);
        stats.put("4.2-Total Q1", executionq1);
        stats.put("4.3-Total Q2", executionq2);
        stats.put("4.4-Total Q3", executionq3);
        stats.put("4.5-Total Max", executionMax);

//        System.out.println("Temps d'execution : ");
//        for (int i = 0; i < processingTimes.size(); i++){
//            System.out.print("\t"+processingTimes.get(i));
//            if (i == q1Index) System.out.print(" : Q1");
//            if (i == q2Index) System.out.print(" : Q2");
//            if (i == q3Index) System.out.print(" : Q3");
//            System.out.println("");
//        }
//        System.out.println("Temps totaux : ");
//        for (int j = 0; j < executionTimes.size(); j++){
//            System.out.print("\t"+executionTimes.get(j));
//            if (j == q1Index) System.out.print(" : Q1");
//            if (j == q2Index) System.out.print(" : Q2");
//            if (j == q3Index) System.out.print(" : Q3");
//            System.out.println("");
//        }


        FileWriter fileWriter = new FileWriter(outPath + "_" + "stats.csv");


        for (String k : stats.keySet()){
            fileWriter.append( k + ";" + stats.get(k) + "\n");
        }


        fileWriter.close();

    }

    private static Float readMsFromLine(String st) {

        int lastSeparator = st.lastIndexOf(';') + 1;
        String valueMs = st.substring(lastSeparator).split(" ")[0];


        return Float.valueOf(valueMs);

    }

    private static void extractTheirData() {
    }

    private static void readParams(String[] args) {
        HashMap<String, String> params = new HashMap<>();

        for (int index = 0; index < args.length; index++){
            String arg = args[index];
            if (arg.startsWith("-")){
                arg = arg.substring(1);

                if ( index+1 < args.length && !args[index+1].startsWith("-") ){
                    params.put(arg, args[index+1]);
                    index++;
                } else {
                    params.put(arg, "true");
                }
            }
        }


        if ( params.get("output") == null || params.get("data") == null || params.get("queries") == null || params.get("jar") == null ){
            System.err.println("Parametre(s) manquant(s). Recu : " + params);
            throw new IllegalArgumentException();
        }

        queriesPath = params.get("queries");
        dataPath = params.get("data");
        outPath = params.get("output");
        verbose = Boolean.parseBoolean(params.get("verbose"));

        group = GroupEnum.US;

        if (params.containsKey("group")){
            if (!params.get("group").equals("us")){
                group = GroupEnum.THEM;
            }
        }

        if (params.containsKey("times")){
            times =  Integer.parseInt( params.get("times") );
        }

        jarPath = params.get("jar");




        jar = new File(jarPath);
        outDir = new File(outPath);
        inData = new File(dataPath);
        inQueries = new File(queriesPath);

    }

    private static void startJar(File jar, File inData, File inQueries, File outDir) {


        Process proc;
        System.out.println("Executing program "+ times + " times");
        try {

            for (int i = 0; i < times; i++) {

                System.out.println("Starting process " + i);
                System.out.println("=======================");
                proc = Runtime.getRuntime().exec("java -Xmx4G -jar " + jar + (verbose ? " -verbose " : "") + getParams(i, inData, inQueries, outDir));


                if (verbose){
                    printProcessOutput( proc.getInputStream(), proc.getErrorStream());
                }

                int retCode =  proc.waitFor();
                if (retCode != 0){
                    System.err.println("Process " + i +  " terminé avec un code de retour " + retCode);
                }else {
                    System.out.println("\n-------------------------");
                    System.out.println("Process " + i + " terminé");
                    System.out.println("-------------------------\n");
                }



            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }

    }

    private static void printProcessOutput(InputStream inputStream, InputStream errorStream) throws IOException {

        byte[] buf = new byte[1024];
        int nr = inputStream.read(buf);
        while (nr != -1)
        {
            System.out.write(buf, 0, nr);
            nr = inputStream.read(buf);
        }

        nr = errorStream.read(buf);
        while (nr != -1)
        {
            System.err.write(buf, 0, nr);
            nr = errorStream.read(buf);
        }
    }

    private static String getParams(int i, File inData, File inQueries, File outDir) {
        String output = outPath + "_" + i;

        return "" +
                "-queries " + inQueries.toPath().toAbsolutePath() + " " +
                "-data " + inData.toPath().toAbsolutePath() + " " +
                "-output "+output + " " +
                "-export_results -export_stats -workload_time";
    }

    enum GroupEnum {
        US, THEM
    }
}

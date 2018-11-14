package fr.kriszt.theo;

import com.sun.javaws.exceptions.InvalidArgumentException;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;

public class Main {

    private static  String queriesPath, dataPath, outPath, jarPath;
    private static int times = 1;
    private static GroupEnum group;

    public static void main(String[] args) {

        /**

         -queries "res/queries" -data "res/dataset" -output "out/wrapper" -jar "res/jar/us.jar" -group "us" -times 1000


         */

        // write your code here

        readParams(args);
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



        File jar, outDir, inData, inQueries;
        jar = new File(jarPath);
        outDir = new File(outPath);
        inData = new File(dataPath);
        inQueries = new File(queriesPath);

        startJar(jar, inData, inQueries, outDir);

    }

    private static void startJar(File jar, File inData, File inQueries, File outDir) {


        ArrayList<InputStream> ins = new ArrayList<>();
        ArrayList<InputStream> errs = new ArrayList<>();
        ArrayList<OutputStream> outs = new ArrayList<>();
        ArrayList<Process> processes = new ArrayList<>();

        Process proc = null;
        try {
            for (int i = 0; i < times; i++) {
                proc = Runtime.getRuntime().exec("java -Xmx4G -jar " + jar + " -verbose " + getParams(i, inData, inQueries, outDir));
                processes.add( proc );
                ins.add(proc.getInputStream());
                errs.add(proc.getErrorStream());
                outs.add(proc.getOutputStream());

                System.out.println("Code de retour du process " + i + " : " + proc.waitFor());



            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        try {
            for (int i = 0; i < processes.size(); i++){
//                Process p = processes.get(i);
                InputStream is = ins.get(i);
                InputStream err = errs.get(i);
//                OutputStream out = outs.get(i);

                byte[] buf = new byte[1024];
                int nr = is.read(buf);
                while (nr != -1)
                {
                    System.out.write(buf, 0, nr);
                    nr = is.read(buf);
                }

                nr = err.read(buf);
                while (nr != -1)
                {
                    System.err.write(buf, 0, nr);
                    nr = err.read(buf);
                }

            }

        } catch (IOException e) {
            e.printStackTrace();
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

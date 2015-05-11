package hadoop.demo;

import com.hadoop.compression.lzo.LzoIndexer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionOutputStream;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

public class Appender {

    public enum PathSelector {
        DATAIN("datain"),
        PARSEFAILED("parse-failed"),
        ERROR("error");

        private String path;

        PathSelector(String rootDir){
            this.path = rootDir;
        }

    }
    public static final String uri = "hdfs://localhost:9000/data/datain";

    /**
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub

/*

        System.out.println(PathSelector.DATAIN.path);
        // get the content user want to append
        String content = "abc";

        // instantiate a configuration class
        Configuration conf = new Configuration();
        for (Map.Entry<String, String> aConf : conf) {
            System.out.println(aConf);
        }

        System.exit(-1);

        // get a HDFS filesystem instance

        FileSystem fs1 = FileSystem.get(conf);
//        System.out.println(fs.listFiles(new Path(uri), true).next());
        //get if file append functionality is enabled
        System.out.println(fs1.getConf());
        System.out.println(fs1.getConf().getResource("hdfs-site.xml"));
        boolean flag = Boolean.getBoolean(fs1.getConf()
                .get("dfs.support.append"));
        System.out.println("dfs.support.append is set to be " + CreateFlag.APPEND.name());
 //       fs.listFiles(new Path("hdfs://localhost:9000/"),false);

        for(int i=0;i<4;i++)
        if (true) {
            Path pathString = new Path("/data/datain/temp1.dat");
            if(!fs1.exists(pathString)) {
                fs1.create(pathString).close();
                    */
/*fs1.close();
                fs1=FileSystem.get(conf);*//*

            }

            FSDataOutputStream fsout = fs1.append(pathString);

            // wrap the outputstream with a writer
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fsout));
            writer.append(content);
            writer.newLine();
            writer.close();
        } else {
            System.err.println("please set the dfs.support.append to be true");
        }

        fs1.close();
*/
        // create am HDFS file system
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        System.loadLibrary("gplcompression");
        // create an output stream to write to a new file in hdfs
        Path outputPath = new Path(
                "/hdfs-push/datain/hdfs.lzo");
        if(!fs.exists(outputPath))
            fs.create(outputPath).close();

        OutputStream outputStream = fs.append(outputPath);

        // now wrap the output stream with a Zlib compression codec
        CompressionCodecFactory codecFactory = new CompressionCodecFactory(conf);
        CompressionCodec codec = codecFactory.getCodecByName("LzopCodec");

        CompressionOutputStream compressedOutput = codec.createOutputStream(outputStream);

        BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(compressedOutput));
        for(int i=0;i<10;i++) {
            bufferedWriter.append("a new String");
            bufferedWriter.newLine();
        }

        bufferedWriter.append("completed 10 times");
        bufferedWriter.newLine();
        bufferedWriter.close();

        LzoIndexer lzoIndexer = new LzoIndexer(conf);
        lzoIndexer.index(outputPath);

    }



}


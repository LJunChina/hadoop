package zkqueue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapred.JobConf;
/**
 * hdfs文件操作帮助类
 * @author Jon_China
 *
 */
public class HdfsDAO {

    private static final String HDFS = "hdfs://hadoop01:9000/";
    private String hdfsPath;
    private Configuration conf;
    private String user;
    public HdfsDAO(Configuration conf) {
        this(HDFS, conf);
    }

    public HdfsDAO(String hdfs, Configuration conf) {
        this.hdfsPath = hdfs;
        this.conf = conf;
    }

    public HdfsDAO(String hdfs, Configuration conf,String user) {
        this.hdfsPath = hdfs;
        this.conf = conf;
        this.user = user;
    }

    public static void main(String[] args) throws IOException {
        JobConf conf = config();
        HdfsDAO hdfs = new HdfsDAO(conf);
        try {
			hdfs.mkdirs("/fileOperation");
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
    }

    public static JobConf config() {
        JobConf conf = new JobConf(HdfsDAO.class);
        conf.setJobName("HdfsDAO");
        conf.addResource("classpath:/hadoop/core-site.xml");
        conf.addResource("classpath:/hadoop/hdfs-site.xml");
        conf.addResource("classpath:/hadoop/mapred-site.xml");
        conf.addResource("classpath:/hadoop/yarn-site.xml");
        return conf;
    }

    public void mkdirs(String folder) throws IOException, InterruptedException {
        Path path = new Path(folder);
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf,"root");
        if (!fs.exists(path)) {
            boolean flag = fs.mkdirs(path);
            System.out.println("Create: " + folder+":"+flag);
        }
        fs.close();
    }

    public void rmr(String folder) throws IOException, InterruptedException {
        Path path = new Path(folder);
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf,"root");
        fs.deleteOnExit(path);
        System.out.println("Delete: " + folder);
        fs.close();
    }

    public void rename(String src, String dst) throws IOException, InterruptedException {
        Path name1 = new Path(src);
        Path name2 = new Path(dst);
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf,"root");
        fs.rename(name1, name2);
        System.out.println("Rename: from " + src + " to " + dst);
        fs.close();
    }

    public void ls(String folder) throws IOException, InterruptedException {
        Path path = new Path(folder);
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf,"root");
        FileStatus[] list = fs.listStatus(path);
        System.out.println("ls: " + folder);
        System.out.println("==========================================================");
        for (FileStatus f : list) {
            System.out.printf("name: %s, folder: %s, size: %d\n", f.getPath(), f.isDirectory(), f.getLen());
        }
        System.out.println("==========================================================");
        fs.close();
    }

    public void createFile(String file, String content) throws IOException, InterruptedException {
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf,"root");
        byte[] buff = content.getBytes();
        FSDataOutputStream os = null;
        try {
            os = fs.create(new Path(file));
            os.write(buff, 0, buff.length);
            System.out.println("Create: " + file);
        } finally {
            if (os != null)
                os.close();
        }
        fs.close();
    }

    public void copyFile(String local, String remote) throws IOException, InterruptedException {
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf,"root");
        fs.copyFromLocalFile(new Path(local), new Path(remote));
        System.out.println("copy from: " + local + " to " + remote);
        fs.close();
    }

    public void download(String remote, String local) throws IOException {
        Path path = new Path(remote);
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
        fs.copyToLocalFile(path, new Path(local));
        System.out.println("download: from" + remote + " to " + local);
        fs.close();
    }
    
    public String cat(String remoteFile) throws IOException {
        Path path = new Path(remoteFile);
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
        FSDataInputStream fsdis = null;
        System.out.println("cat: " + remoteFile);
        
        OutputStream baos = new ByteArrayOutputStream(); 
        String str = null;
        
        try {
            fsdis = fs.open(path);
            IOUtils.copyBytes(fsdis, baos, 4096, false);
            str = baos.toString();  
        } finally {
            IOUtils.closeStream(fsdis);
            fs.close();
        }
        System.out.println(str);
        return str;
    }
}

package hbase.job;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.Random;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
/**
 * @version 2012-08-28
 * 
 * @author kuangzuoqiang
 *
 */
public class JobUtil {
	
	private static final Random RANDOM = new Random();
	
	/**
	 * 
	 * @param mapreduce	任务中用到的类
	 * @param confs	配置文件路径，从类跟目录开始查找
	 * @param tool
	 * @param args	参数
	 * @return
	 * @throws Exception
	 */
	public static int runJob(Class[] classes, String[] confs,Tool tool, String[] args) throws Exception{
		Configuration conf = HBaseConfiguration.create();
		JobConf j= new JobConf(conf);
		
		j.setJar(createJarFile(classes));
		return ToolRunner.run(j, tool, args);
	}
	
	/**
	 * 将mapredurce任务中用到的类打包成jar包，并返回路径
	 * @param classes
	 * @return	jar文件的路径
	 * @throws IOException
	 */
	private static String createJarFile(Class[] classes) throws IOException{
		//得到输出临时目录
		String tmpdir = System.getProperty("java.io.tmpdir");
		clearTmpDir(tmpdir);
		//临时jar生产路径
		String  jarPath = tmpdir +  File.separator + "job_" + new Date().getTime() 
				+ "_" + RANDOM.nextInt(10000) + ".jar";
		//创建jar包manifest文件
		Manifest manifest = new Manifest();
		//设置版本信息
		manifest.getMainAttributes().putValue("Manifest-Version", "1.0");
		//用manifest对象创建jar文件输出流
		JarOutputStream jarOutPut = new JarOutputStream(new FileOutputStream(jarPath), manifest);
		//将所有的类压缩至jar文件流中
		for(Class cls : classes){
			//通过类名找到类文件,将"."替换成"/"
			String clsPath = cls.getName().replaceAll("\\.", "/") + ".class";
			JarEntry jarEntry = new JarEntry(clsPath);
			InputStream is = cls.getClassLoader().getResourceAsStream(clsPath);
			jarOutPut.putNextEntry(jarEntry);
			
			byte[] buffer = new byte[1024];
			int len = 0;
			while((len = is.read(buffer)) > 0){
				jarOutPut.write(buffer, 0, len);
			}
			jarOutPut.flush();
			jarOutPut.closeEntry();
		}
		
		jarOutPut.flush();
		jarOutPut.close();
		//返回临时jar文件路径
		return jarPath;
	}
	
	/**
	 * 除临时文件
	 * @param tmpDirName	临时目录
	 */
	private static void clearTmpDir(String tmpDirName){
		File tmpDir = new File(tmpDirName);
		File[] files = tmpDir.listFiles();
		for(File file : files){
			String fileName = file.getName();
			if(fileName.startsWith("job_") && (fileName.endsWith(".jar") || fileName.endsWith(".xml"))){
				try{
					file.delete();
				} catch (Exception e) {
					
				}
				
			}
		}
	}
	
}

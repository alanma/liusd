package hbase.parallel;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * <p>描述:  </p>
 *
 * <p>原则:   </p>
 *
 * <p>作用:   </p>
 *
 * <p>Create Time:  2012-11-7 <p>
 *
 * @author liusheding
 * @version 1.1
 */
public class ParallelScanner {
	public static void main(String[] args) throws IOException {
		// Configuration conf = HBaseConfiguration.create();
		// HTableInterface table = new HTable(conf, "cups_1");
		// HRegionInfo region = new HRegionInfo("cups_1".getBytes());
		// HRegion regio = new HRegion();
		ConcurrentMap<String, String> map = new ConcurrentHashMap<String, String>();
		int n = 5;
		ExecutorService es = Executors.newFixedThreadPool(n);
		// ConcurrentMap<Integer, Future<String>> r= new
		// ConcurrentHashMap<Integer,Future<String>>();
		Map<Integer, Future<String>> r = new HashMap<Integer, Future<String>>();
		for (int i = 0; i < 10; i++) {
			final int j = i;
			Future<String> f = es.submit(new Callable<String>() {
				public String call() throws Exception {
					Thread.sleep(2000);
					return "fff" + j;
				}
			});
			// r.putIfAbsent(i, f);
			r.put(i, f);
		}
		try {
			for (Map.Entry<Integer, Future<String>> entry : r.entrySet()) {
				for (;;) {
					if (!entry.getValue().isDone()) {
						System.out.println("waiting for task to done");
						Thread.sleep(100);
					} else {
						System.out.println("task is  done");
						break;
					}
				}
				System.out.println(entry.getKey() + ":"
						+ entry.getValue().get());
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		} finally {
			es.shutdown();
		}
		if (!es.isShutdown()) {
			es.shutdownNow();
		}
	}

}

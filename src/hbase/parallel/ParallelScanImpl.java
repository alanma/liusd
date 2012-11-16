package hbase.parallel;

import java.util.concurrent.ConcurrentMap;

/**
 * <p>描述:  </p>
 *
 * <p>原则:   </p>
 *
 * <p>作用:   </p>
 *
 * <p>Create Time:  2012-11-9 <p>
 *
 * @author liusheding
 * @version 1.1
 */
public class  ParallelScanImpl<T> implements ParallelScanInterface<T> {

	public ConcurrentMap<byte[],T> scan() {
		return null;
	}
}

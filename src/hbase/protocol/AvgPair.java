package hbase.protocol;


/**
 * <p>描述:  </p>
 *
 * <p>原则:   </p>
 *
 * <p>作用: 求平均对.  </p>
 *
 * <p>Create Time:  2012-10-24 <p>
 *
 * @author liusheding
 * @version 1.1
 */
public class AvgPair  {
	//数目
	private Long count;
	//总数
	private Long sum;
	/**
	 * 默认构造函数
	 * count = 0L;
	 * sum = 0L;
	 */
	public AvgPair() {
		this.count = 0L;
		sum = 0L;
	}
	/**
	 * 总数构造函数
	 * sum = param;
	 * count = 1L;
	 * @param sum
	 */
	public AvgPair(Long sum) {
		this.count = 1L;
		this.sum = sum;
	}
	/**
	 * 构造函数,字段全部
	 * @param sum
	 * @param count
	 */
	public AvgPair(Long sum,Long count) {
		this.count = count;
		this.sum = sum;
	}
	/**
	 * 更新pair
	 * @param value
	 */
	public void update(Long value){
		this.sum += value;
		this.count += 1L;
	}
	/**
	 * 获取数学平均值
	 * @return sum/(count+0.00)
	 */
	public Double getAvg(){
		return sum/(count+0.00);
	}
	
	public Long getCount() {
		return count;
	}
	public void setCount(Long count) {
		this.count = count;
	}
	public Long getSum() {
		return sum;
	}
	public void setSum(Long sum) {
		this.sum = sum;
	}
	
}

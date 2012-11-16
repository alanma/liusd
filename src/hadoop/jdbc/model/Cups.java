package hadoop.jdbc.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

/**
 * @author liusheding
 * @version 1.0
 * @create_date 2012-8-31
 */
public class Cups implements Writable, DBWritable {

	// private Long id; // 序列
	public String priAcctNoConv; // 交易卡号
	public String cardAttr; // 卡性质
	public String cupsCardIn; // 是否银联标准卡
	public String acptInsIdCd; // 受理机构标识码
	public String fwdInsIdCd; // 发送机构标识码
	public String sysTraNo; // 系统跟踪号
	public String issInsIdCd; // 发卡机构标识码
	public String transId; // 交易代码
	public String tfrDtTm; // 交易传输时间
	public Long settleAt; // 清算金额
	public String termId; // 终端号
	public String mchntCd; // 商户标识代码
	public String mchntTp; // 商户类型
	public String regionCd; // 地区代码
	public String transCh; // 交易渠道
	public String acptTermTp; // 受理终端类型

	// private String bigMcc; // 交易大MCC
	// private String feeType; // 费率

	@Override
	public void write(PreparedStatement statement) throws SQLException {
		// statement.setLong(1, id);
		statement.setString(1, priAcctNoConv);
		statement.setString(2, cardAttr);
		statement.setString(3, cupsCardIn);
		statement.setString(4, acptInsIdCd);
		statement.setString(5, fwdInsIdCd);
		statement.setString(6, sysTraNo);
		statement.setString(7, issInsIdCd);
		statement.setString(8, transId);
		statement.setString(9, tfrDtTm);
		statement.setLong(10, settleAt);
		statement.setString(11, termId);
		statement.setString(12, mchntCd);
		statement.setString(13, mchntTp);
		statement.setString(14, regionCd);
		statement.setString(15, transCh);
		statement.setString(16, acptTermTp);
		// statement.setString(18, bigMcc);
		// statement.setString(19, feeType);
	}

	@Override
	public void readFields(ResultSet resultSet) throws SQLException {
		// id = resultSet.getLong(1);
		priAcctNoConv = resultSet.getString(1);
		cardAttr = resultSet.getString(2);
		cupsCardIn = resultSet.getString(3);
		acptInsIdCd = resultSet.getString(4);
		fwdInsIdCd = resultSet.getString(5);
		sysTraNo = resultSet.getString(6);
		issInsIdCd = resultSet.getString(7);
		transId = resultSet.getString(8);
		tfrDtTm = resultSet.getString(9);
		settleAt = resultSet.getLong(10);
		termId = resultSet.getString(11);
		mchntCd = resultSet.getString(12);
		mchntTp = resultSet.getString(13);
		regionCd = resultSet.getString(14);
		transCh = resultSet.getString(15);
		acptTermTp = resultSet.getString(16);
		// bigMcc = resultSet.getString(17);
		// feeType = resultSet.getString(18);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// out.writeLong(id);
		Text.writeString(out, priAcctNoConv);
		Text.writeString(out, cardAttr);
		Text.writeString(out, cupsCardIn);
		Text.writeString(out, acptInsIdCd);
		Text.writeString(out, fwdInsIdCd);
		Text.writeString(out, sysTraNo);
		Text.writeString(out, issInsIdCd);
		Text.writeString(out, transId);
		Text.writeString(out, tfrDtTm);
		out.writeLong(settleAt);
		Text.writeString(out, termId);
		Text.writeString(out, mchntCd);
		Text.writeString(out, mchntTp);
		Text.writeString(out, regionCd);
		Text.writeString(out, transCh);
		Text.writeString(out, acptTermTp);
		// Text.writeString(out, bigMcc);
		// Text.writeString(out, feeType);

	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// id = in.readLong();
		priAcctNoConv = Text.readString(in);
		cardAttr = Text.readString(in);
		cupsCardIn = Text.readString(in);
		acptInsIdCd = Text.readString(in);
		fwdInsIdCd = Text.readString(in);
		sysTraNo = Text.readString(in);
		issInsIdCd = Text.readString(in);
		transId = Text.readString(in);
		tfrDtTm = Text.readString(in);
		settleAt = in.readLong();
		termId = Text.readString(in);
		mchntCd = Text.readString(in);
		mchntTp = Text.readString(in);
		regionCd = Text.readString(in);
		transCh = Text.readString(in);
		acptTermTp = Text.readString(in);
		// bigMcc = Text.readString(in);
		// feeType = Text.readString(in);
	}

	public Cups() {

	}

	public Cups(String[] line)  {
		Field[] fields = this.getClass().getDeclaredFields();
		if(fields.length-1 == line.length ){
			for ( int i = 0; i< line.length; i++){
				setValue(i,line[i]);
			}
		}

	}

	public final static String zero[] = { "0", "00", "000", "0000", "00000",
			"000000", "0000000", "00000000", "000000000", "0000000000" };

	/**
	 * key = 卡号长度+卡号（不足低位补0，总长21位）+商户代码（15位）+交易传输日期（14位）+系统跟踪号 （6位） = 56
	 * 位长度字符串数字
	 * 
	 * @return
	 */
	public String[] showKeys() {
		if (priAcctNoConv == null || mchntCd == null || sysTraNo == null
				|| tfrDtTm == null)
			return null;
		StringBuilder[] sb = new StringBuilder[2];
		String card = Integer.toString(priAcctNoConv.length()) + priAcctNoConv;
		// TODO 这里以后需要严格验证 ,取0数组时不验证时会发生数组越界
		if (priAcctNoConv.length() < 19) {
			card += zero[19 - priAcctNoConv.length() - 1];
		}
		// cupsKey
		sb[0] = new StringBuilder();
		sb[0].append(card);
		sb[0].append(tfrDtTm); // 调整日期的位置
		sb[0].append(mchntCd);
		sb[0].append(sysTraNo);
		// indexKey
		sb[1] = new StringBuilder();
		sb[1].append(mchntCd);
		sb[1].append(tfrDtTm);
		sb[1].append(card);
		sb[1].append(sysTraNo);
		String[] keys = { sb[0].toString(), sb[1].toString() };
		return keys;
	}

	public Object getValue(int index) {
		switch (index) {
		case 0:
			return getPriAcctNoConv();
		case 1:
			return getCardAttr();
		case 2:
			return getCupsCardIn();
		case 3:
			return getAcptInsIdCd();
		case 4:
			return getFwdInsIdCd();
		case 5:
			return getSysTraNo();
		case 6:
			return getIssInsIdCd();
		case 7:
			return getTransId();
		case 8:
			return getTfrDtTm();
		case 9:
			return getSettleAt();
		case 10:
			return getTermId();
		case 11:
			return getMchntCd();
		case 12:
			return getMchntTp();
		case 13:
			return getRegionCd();
		case 14:
			return getTransCh();
		case 15:
			return getAcptTermTp();
		default:
			return null;
		}
	}
	public void setValue(int index,String value){
		switch (index) {
		case 0:
			setPriAcctNoConv(value);
			break;
		case 1:
			setCardAttr(value);
			break;
		case 2:
			setCupsCardIn(value);
			break;
		case 3:
			setAcptInsIdCd(value);
			break;
		case 4:
			setFwdInsIdCd(value);
			break;
		case 5:
			setSysTraNo(value);
			break;
		case 6:
			setIssInsIdCd(value);
			break;
		case 7:
			setTransId(value);
			break;
		case 8:
			setTfrDtTm(value);
			break;
		case 9:
			setSettleAt(Long.parseLong(value));
			break;
		case 10:
			setTermId(value);
			break;
		case 11:
			setMchntCd(value);
			break;
		case 12:
			setMchntTp(value);
			break;
		case 13:
			setRegionCd(value);
			break;
		case 14:
			setTransCh(value);
			break;
		case 15:
			setAcptTermTp(value);
			break;
		default:
			break;
		}
	}

	public String getPriAcctNoConv() {
		return priAcctNoConv;
	}

	public void setPriAcctNoConv(String priAcctNoConv) {
		this.priAcctNoConv = priAcctNoConv;
	}

	public String getCardAttr() {
		return cardAttr;
	}

	public void setCardAttr(String cardAttr) {
		this.cardAttr = cardAttr;
	}

	public String getCupsCardIn() {
		return cupsCardIn;
	}

	public void setCupsCardIn(String cupsCardIn) {
		this.cupsCardIn = cupsCardIn;
	}

	public String getAcptInsIdCd() {
		return acptInsIdCd;
	}

	public void setAcptInsIdCd(String acptInsIdCd) {
		this.acptInsIdCd = acptInsIdCd;
	}

	public String getFwdInsIdCd() {
		return fwdInsIdCd;
	}

	public void setFwdInsIdCd(String fwdInsIdCd) {
		this.fwdInsIdCd = fwdInsIdCd;
	}

	public String getSysTraNo() {
		return sysTraNo;
	}

	public void setSysTraNo(String sysTraNo) {
		this.sysTraNo = sysTraNo;
	}

	public String getIssInsIdCd() {
		return issInsIdCd;
	}

	public void setIssInsIdCd(String issInsIdCd) {
		this.issInsIdCd = issInsIdCd;
	}

	public String getTransId() {
		return transId;
	}

	public void setTransId(String transId) {
		this.transId = transId;
	}

	public String getTfrDtTm() {
		return tfrDtTm;
	}

	public void setTfrDtTm(String tfrDtTm) {
		this.tfrDtTm = tfrDtTm;
	}

	public Long getSettleAt() {
		return settleAt;
	}

	public void setSettleAt(Long settleAt) {
		this.settleAt = settleAt;
	}

	public String getTermId() {
		return termId;
	}

	public void setTermId(String termId) {
		this.termId = termId;
	}

	public String getMchntCd() {
		return mchntCd;
	}

	public void setMchntCd(String mchntCd) {
		this.mchntCd = mchntCd;
	}

	public String getMchntTp() {
		return mchntTp;
	}

	public void setMchntTp(String mchntTp) {
		this.mchntTp = mchntTp;
	}

	public String getRegionCd() {
		return regionCd;
	}

	public void setRegionCd(String regionCd) {
		this.regionCd = regionCd;
	}

	public String getTransCh() {
		return transCh;
	}

	public void setTransCh(String transCh) {
		this.transCh = transCh;
	}

	public String getAcptTermTp() {
		return acptTermTp;
	}

	public void setAcptTermTp(String acptTermTp) {
		this.acptTermTp = acptTermTp;
	}

}
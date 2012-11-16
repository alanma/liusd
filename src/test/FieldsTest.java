package test;

import hadoop.jdbc.model.Cups;

import java.lang.reflect.Field;

import org.jruby.compiler.ir.instructions.GetClassVariableInstr;

/**
 * @author liusheding
 * @version 1.0
 * @create_date 2012-9-13
 */
public class FieldsTest {
	private String a;
	private String b;
	private String priAcctNoConv; // 交易卡号
	private String cardAttr; // 卡性质
	private String cupsCardIn; // 是否银联标准卡
	private String acptInsIdCd; // 受理机构标识码
	private String fwdInsIdCd; // 发送机构标识码
	private String sysTraNo; // 系统跟踪号
	private String issInsIdCd; // 发卡机构标识码
	private String transId; // 交易代码
	private String tfrDtTm; // 交易传输时间
	private Long settleAt; // 清算金额
	private String termId; // 终端号
	private String mchntCd; // 商户标识代码
	private String mchntTp; // 商户类型
	private String regionCd; // 地区代码
	private String transCh; // 交易渠道
	private String acptTermTp; // 受理终端类型

	public static void main(String[] args) throws IllegalArgumentException,
			IllegalAccessException {
		Cups f = new Cups();
		Field[] f1 = f.getClass().getDeclaredFields();
		f1[0].set(f, "dasd");
		System.out.println(f.getClass().getFields());
	}

	public String getA() {
		return a;
	}

	public void setA(String a) {
		this.a = a;
	}

	public String getB() {
		return b;
	}

	public void setB(String b) {
		this.b = b;
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

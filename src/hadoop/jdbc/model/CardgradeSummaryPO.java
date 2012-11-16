package hadoop.jdbc.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

/**
 *@author liusheding
 *@version 1.0
 *@create_date 2012-8-30
 */
public class CardgradeSummaryPO implements Writable, DBWritable {

	
	public String cardgrade;
	
	public int num;
	
	@Override
	public void write(PreparedStatement statement) throws SQLException {
		statement.setInt(2, num);
		statement.setString(1, cardgrade);
	}

	@Override
	public void readFields(ResultSet resultSet) throws SQLException {
		this.cardgrade = resultSet.getString(1);
		this.num = resultSet.getInt(2);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(num);
		Text.writeString(out, cardgrade);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.cardgrade = Text.readString(in);
		this.num = in.readInt();
	}

	public String getCardgrade() {
		return cardgrade;
	}

	public void setCardgrade(String cardgrade) {
		this.cardgrade = cardgrade;
	}

	public int getNum() {
		return num;
	}

	public void setNum(int num) {
		this.num = num;
	}
	
	public CardgradeSummaryPO(String grade,int num){
		this.cardgrade = grade;
		this.num = num;
	}
	public String toString(){
		return "卡级别：" +this.getCardgrade()+",数量："+this.getNum();
	}
	

}

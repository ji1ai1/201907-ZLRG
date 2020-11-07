package jiangli.rg.rg_preprocess;

class RgPreprocessUserMapper extends com.aliyun.odps.mapred.MapperBase
{
	public void setup(TaskContext context) throws java.io.IOException
	{
	}

	public void map(long recordNum, com.aliyun.odps.data.Record record, TaskContext context) throws java.io.IOException
	{
		com.aliyun.odps.data.Record output_record = context.createOutputRecord();

		output_record.setString("用戶標識", record.getString("user_id"));
		output_record.setString("用戶城市", record.getString("live_city_id"));
		output_record.setString("用戶期望城市", record.getString("desire_jd_city_id"));
		output_record.setString("用戶期望行業", record.getString("desire_jd_industry_id"));
		output_record.setString("用戶期望職類", record.getString("desire_jd_type_id"));

		String 用戶期望薪水 = record.getString("desire_jd_salary_id");
		output_record.setDouble("用戶期望最低薪水", 最低薪水_hashmap.getOrDefault(用戶期望薪水, null));
		output_record.setDouble("用戶期望最高薪水", 最高薪水_hashmap.getOrDefault(用戶期望薪水, null));

		output_record.setString("用戶當前行業", record.getString("cur_industry_id"));
		output_record.setString("用戶當前職類", record.getString("cur_jd_type"));

		String 用戶當前薪水 = record.getString("cur_salary_id");
		output_record.setDouble("用戶當前最低薪水", 最低薪水_hashmap.getOrDefault(用戶當前薪水, null));
		output_record.setDouble("用戶當前最高薪水", 最高薪水_hashmap.getOrDefault(用戶當前薪水, null));

		output_record.setDouble("用戶學歷值", 學歷_hashmap.getOrDefault(record.getString("cur_degree_id"), null));
		try
		{
			output_record.setDouble("用戶年齡", Double.parseDouble(record.getString("birthday")));
		}
		catch (NumberFormatException e)
		{
			output_record.setDouble("用戶年齡", null);
		}
		try
		{
			double 用戶開始工作月 = Double.parseDouble(record.getString("start_work_date"));
			output_record.setDouble("用戶工作年限", ((9 + 12 * 2019) - (用戶開始工作月 % 100 + 12 * Math.floor(用戶開始工作月 / 100))) / 12D);
		}
		catch (NumberFormatException e)
		{
			output_record.setDouble("用戶工作年限", null);
		}
		output_record.setString("用戶經驗", record.getString("experience"));

		context.write(output_record);
	}


	private final static java.util.HashMap<String, Double> 最低薪水_hashmap = new java.util.HashMap<String, Double>()
	{
		{
			//put("0000000000", -1D);
			put("0000001000", 0D);
			put("0100002000", 1000D);
			put("0200104000", 2001D);
			put("0400106000", 4001D);
			put("0600108000", 6001D);
			put("0800110000", 8001D);
			put("1000115000", 10001D);
			put("1500120000", 15001D);
			put("1500125000", 15002D);
			put("2000130000", 20001D);
			put("2500135000", 25001D);
			put("2500199999", 25002D);
			put("3000150000", 30001D);
			put("3500150000", 35001D);
			put("5000170000", 50001D);
			put("70001100000", 70001D);
			put("100001150000", 100000D);
		}
	};

	private final static java.util.HashMap<String, Double> 最高薪水_hashmap = new java.util.HashMap<String, Double>()
	{
		{
			//put("0000000000", -1D);
			put("0000001000", 1000D);
			put("0100002000", 2000D);
			put("0200104000", 4000D);
			put("0400106000", 6000D);
			put("0600108000", 8000D);
			put("0800110000", 10000D);
			put("1000115000", 15000D);
			put("1500120000", 20000D);
			put("1500125000", 25000D);
			put("2000130000", 30000D);
			put("2500135000", 35000D);
			put("2500199999", 99999D);
			put("3000150000", 50000D);
			put("3500150000", 50000D);
			put("5000170000", 70000D);
			put("70001100000", 100000D);
			put("100001150000", 150000D);
		}
	};

	private final static java.util.HashMap<String, Double> 學歷_hashmap = new java.util.HashMap<String, Double>()
	{
		{
			put("其他", 0D);
			put("初中", 1D);
			put("中技", 2D);
			put("中专", 2D);
			put("高中", 2D);
			put("大专", 2.9);
			put("本科", 3D);
			put("硕士", 4D);
			put("MBA", 4.1);
			put("EMBA", 4.2);
			put("博士", 5D);
		}
	};
}

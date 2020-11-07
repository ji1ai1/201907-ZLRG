package jiangli.rg.rg_preprocess;

class RgPreprocessJdMapper extends com.aliyun.odps.mapred.MapperBase
{
	public void setup(TaskContext context) throws java.io.IOException
	{
	}

	public void map(long recordNum, com.aliyun.odps.data.Record record, TaskContext context) throws java.io.IOException
	{
		com.aliyun.odps.data.Record output_record = context.createOutputRecord();

		output_record.setString("職位標識", record.getString("jd_no"));
		output_record.setString("職位標題", record.getString("jd_title"));
		output_record.setString("職位城市", record.getString("city"));
		output_record.setString("職位子類", record.getString("jd_sub_type"));
		try
		{
			output_record.setDouble("職位需求人數", Double.parseDouble(record.getString("require_nums")));
		}
		catch (NumberFormatException e)
		{
			output_record.setDouble("職位需求人數", null);
		}
		try
		{
			output_record.setDouble("職位最低薪水", Double.parseDouble(record.getString("min_salary")));
		}
		catch (NumberFormatException e)
		{
			output_record.setDouble("職位最低薪水", null);
		}
		try
		{
			output_record.setDouble("職位最高薪水", Double.parseDouble(record.getString("max_salary")));
		}
		catch (NumberFormatException e)
		{
			output_record.setDouble("職位最高薪水", null);
		}

		java.text.SimpleDateFormat simple_date_format= new java.text.SimpleDateFormat("yyyyMMdd");
		try
		{
			java.util.Date 基日期 = simple_date_format.parse("20190901");
			java.util.Date 日期 = simple_date_format.parse(record.getString("start_date"));
			output_record.setDouble("職位開始日序", (日期.getTime() - 基日期.getTime()) / 86400000D);
		}
		catch (java.text.ParseException e)
		{
			throw new java.io.IOException(record.getString("start_date"));
		}

		try
		{
			java.util.Date 基日期 = simple_date_format.parse("20190901");
			java.util.Date 日期 = simple_date_format.parse(record.getString("end_date"));
			output_record.setDouble("職位終止日序", (日期.getTime() - 基日期.getTime()) / 86400000D);
		}
		catch (java.text.ParseException e)
		{
			throw new java.io.IOException(record.getString("end_date"));
		}

		try
		{
			output_record.setDouble("職位出差否", Double.parseDouble(record.getString("is_travel")));
		}
		catch (NumberFormatException e)
		{
			output_record.setDouble("職位出差否", null);
		}
		output_record.setDouble("職位工作年限", 工作年限_hashmap.getOrDefault(record.getString("min_years"), null));
		output_record.setString("職位關鍵詞", record.getString("key"));
		output_record.setDouble("職位最低學歷值", 學歷_hashmap.getOrDefault(record.getString("min_edu_level"), null));
		output_record.setString("職位管理經驗", record.getString("is_mangerial"));
		output_record.setString("職位語言需求", record.getString("resume_language_required"));
		output_record.setString("職位描述", record.getString("job_description"));

		context.write(output_record);
	}

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

	private final static java.util.HashMap<String, Double> 工作年限_hashmap = new java.util.HashMap<String, Double>()
	{
		{
			put("0", 0D);
			put("1", 1D);
			put("103", 1D);
			put("2", 2D);
			put("3", 3D);
			put("305", 3D);
			put("399", 3D);
			put("5", 5D);
			put("510", 5D);
			put("8", 8D);
			put("899", 8D);
			put("1099", 10D);
		}
	};
}

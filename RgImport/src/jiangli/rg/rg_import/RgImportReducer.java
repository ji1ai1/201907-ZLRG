package jiangli.rg.rg_import;

class RgImportReducer extends com.aliyun.odps.mapred.ReducerBase
{
	public void setup(TaskContext context) throws java.io.IOException
	{
	}

	public void reduce(com.aliyun.odps.data.Record key_record, java.util.Iterator<com.aliyun.odps.data.Record> value_records, TaskContext context) throws java.io.IOException
	{
		com.aliyun.odps.data.Record output_record = context.createOutputRecord();

		com.aliyun.odps.data.Record first_value_record = value_records.next();
		String 首用戶標識 =  first_value_record.getString("用戶標識");
		String 首職位標識 = first_value_record.getString("職位標識");
		long 首瀏覽 = first_value_record.getBigint("瀏覽");
		long 首投遞 = first_value_record.getBigint("投遞");
		long 首認可 = first_value_record.getBigint("認可");

		long 記錄標識 = 0;
		while (value_records.hasNext())
		{
			com.aliyun.odps.data.Record value_record = value_records.next();

			output_record.setString("用戶標識", value_record.getString("用戶標識"));
			output_record.setString("職位標識", value_record.getString("職位標識"));
			output_record.setBigint("記錄標識", 記錄標識);
			output_record.setBigint("瀏覽", value_record.getBigint("瀏覽"));
			output_record.setBigint("投遞", value_record.getBigint("投遞"));
			output_record.setBigint("認可", value_record.getBigint("認可"));

			context.write(output_record);
			記錄標識++;
		}

		output_record.setString("用戶標識", 首用戶標識);
		output_record.setString("職位標識", 首職位標識);
		output_record.setBigint("記錄標識", 記錄標識);
		output_record.setBigint("瀏覽", 首瀏覽);
		output_record.setBigint("投遞", 首投遞);
		output_record.setBigint("認可", 首認可);

		context.write(output_record);
	}
}

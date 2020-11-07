package jiangli.rg.rg_import;

class RgImportMapper extends com.aliyun.odps.mapred.MapperBase
{
	public void setup(TaskContext context) throws java.io.IOException
	{
	}

	public void map(long recordNum, com.aliyun.odps.data.Record record, TaskContext context) throws java.io.IOException
	{
		com.aliyun.odps.data.Record key_record = context.createMapOutputKeyRecord();
		com.aliyun.odps.data.Record value_record = context.createMapOutputValueRecord();

		key_record.setBigint("零", 0L);
		if (record.getColumnCount() == 5)
		{
			value_record.setString("用戶標識", record.getString("user_id"));
			value_record.setString("職位標識", record.getString("jd_no"));
			value_record.setBigint("瀏覽", Long.parseLong(record.getString("browsed")));
			value_record.setBigint("投遞", Long.parseLong(record.getString("delivered")));
			value_record.setBigint("認可", Long.parseLong(record.getString("satisfied")));
		}
		else
		{
			value_record.setString("用戶標識", record.getString("user_id"));
			value_record.setString("職位標識", record.getString("jd_no"));
			value_record.setBigint("瀏覽", -1L);
			value_record.setBigint("投遞", -1L);
			value_record.setBigint("認可", -1L);
		}

		context.write(key_record, value_record);
	}
}

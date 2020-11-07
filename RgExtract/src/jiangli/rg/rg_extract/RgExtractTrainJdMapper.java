package jiangli.rg.rg_extract;

class RgExtractTrainJdMapper extends com.aliyun.odps.mapred.MapperBase
{
	public void setup(TaskContext context) throws java.io.IOException
	{
	}

	public void map(long recordNum, com.aliyun.odps.data.Record record, TaskContext context) throws java.io.IOException
	{
		com.aliyun.odps.data.Record key_record = context.createMapOutputKeyRecord();
		com.aliyun.odps.data.Record value_record = context.createMapOutputValueRecord();

		int 組數 = Integer.parseInt(context.getInputTableInfo().getLabel());

		String 用戶標識 = record.getString("用戶標識");
		for (int 甲 = 0; 甲 < 組數; 甲++)
		{
			key_record.setBigint("欄0", (long)甲);
			value_record.setString("職位標識", record.getString("職位標識"));
			value_record.setString("職位標題", record.getString("職位標題"));
			value_record.setString("職位子類", record.getString("職位子類"));
			value_record.setString("職位城市", record.getString("職位城市"));
			value_record.setString("職位描述", record.getString("職位描述"));
			value_record.setString("用戶標識", 用戶標識);
			if (record.getBigint("欄0") == 甲)
				value_record.setBigint("標特標誌", 0L);
			else
				value_record.setBigint("標特標誌", 1L);
			value_record.setBigint("瀏覽", record.getBigint("瀏覽"));
			value_record.setBigint("投遞", record.getBigint("投遞"));
			value_record.setBigint("認可", record.getBigint("認可"));

			context.write(key_record, value_record);
		}
	}
}

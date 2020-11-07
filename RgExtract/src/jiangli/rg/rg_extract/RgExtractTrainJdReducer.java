package jiangli.rg.rg_extract;

class RgExtractTrainJdReducer extends com.aliyun.odps.mapred.ReducerBase
{
	public void setup(TaskContext context) throws java.io.IOException
	{
	}

	public void reduce(com.aliyun.odps.data.Record key_record, java.util.Iterator<com.aliyun.odps.data.Record> value_records, TaskContext context) throws java.io.IOException
	{
		com.aliyun.odps.data.Record output_record = context.createOutputRecord();
		output_record.setBigint("欄0", key_record.getBigint("欄0"));

		java.util.HashMap<String, RgExtractJd> 標籤職位標識_hashmap = new java.util.HashMap<String, RgExtractJd>();
		java.util.HashMap<String, RgExtractTrainJd> 特征職位標識_hashmap = new java.util.HashMap<String, RgExtractTrainJd>();
		java.util.HashMap<String, RgExtractTrainJd> 特征職位標題_hashmap = new java.util.HashMap<String, RgExtractTrainJd>();
		java.util.HashMap<String, RgExtractTrainJd> 特征職位子類_hashmap = new java.util.HashMap<String, RgExtractTrainJd>();
		java.util.HashMap<String, RgExtractTrainJd> 特征職位城市_hashmap = new java.util.HashMap<String, RgExtractTrainJd>();
		java.util.HashMap<String, RgExtractTrainJd> 特征職位描述_hashmap = new java.util.HashMap<String, RgExtractTrainJd>();
		while (value_records.hasNext())
		{
			com.aliyun.odps.data.Record value_record = value_records.next();

			String 職位標識 = value_record.getString("職位標識");
			String 職位標題 = value_record.getString("職位標題");
			String 職位子類 = value_record.getString("職位子類");
			String 職位城市 = value_record.getString("職位城市");
			String 職位描述 = value_record.getString("職位描述");
			if (value_record.getBigint("標特標誌") == 0)
			{
				if (標籤職位標識_hashmap.containsKey(職位標識))
					continue;

				RgExtractJd extract_jd = new RgExtractJd();
				extract_jd.職位標題 = 職位標題;
				extract_jd.職位子類 = 職位子類;
				extract_jd.職位城市 = 職位城市;
				extract_jd.職位描述 = 職位描述;
				標籤職位標識_hashmap.put(職位標識, extract_jd);
			}
			else
			{
				String 用戶標識 = value_record.getString("用戶標識");
				long 瀏覽 = value_record.getBigint("瀏覽");
				long 投遞 = value_record.getBigint("投遞");
				long 認可 = value_record.getBigint("認可");

				if (!特征職位標識_hashmap.containsKey(職位標識))
					特征職位標識_hashmap.put(職位標識, new RgExtractTrainJd());
				if (!特征職位標題_hashmap.containsKey(職位標題))
					特征職位標題_hashmap.put(職位標題, new RgExtractTrainJd());
				if (!特征職位子類_hashmap.containsKey(職位子類))
					特征職位子類_hashmap.put(職位子類, new RgExtractTrainJd());
				if (!特征職位城市_hashmap.containsKey(職位城市))
					特征職位城市_hashmap.put(職位城市, new RgExtractTrainJd());
				if (!特征職位描述_hashmap.containsKey(職位描述))
					特征職位描述_hashmap.put(職位描述, new RgExtractTrainJd());

				RgExtractTrainJd extract_train_jd = 特征職位標識_hashmap.get(職位標識);
				extract_train_jd.記錄數++;
				if (瀏覽 == 1)
					extract_train_jd.瀏覽數++;
				if (投遞 == 1)
					extract_train_jd.投遞數++;
				if (認可 == 1)
					extract_train_jd.認可數++;

				extract_train_jd = 特征職位標題_hashmap.get(職位標題);
				extract_train_jd.記錄數++;
				if (認可 == 1)
					extract_train_jd.認可數++;

				extract_train_jd = 特征職位子類_hashmap.get(職位子類);
				extract_train_jd.記錄數++;
				if (認可 == 1)
					extract_train_jd.認可數++;

				extract_train_jd = 特征職位城市_hashmap.get(職位城市);
				extract_train_jd.記錄數++;
				if (認可 == 1)
					extract_train_jd.認可數++;

				extract_train_jd = 特征職位描述_hashmap.get(職位描述);
				extract_train_jd.記錄數++;
				if (認可 == 1)
					extract_train_jd.認可數++;
			}
		}

		for (String 職位標識 : 標籤職位標識_hashmap.keySet())
		{
			if (特征職位標識_hashmap.containsKey(職位標識))
			{
				RgExtractJd extract_jd = 標籤職位標識_hashmap.get(職位標識);
				output_record.setString("職位標識", 職位標識);

				RgExtractTrainJd extract_train_jd = 特征職位標識_hashmap.get(職位標識);
				output_record.setDouble("職0", extract_train_jd.認可數);
				output_record.setDouble("職1", extract_train_jd.投遞數 / extract_train_jd.記錄數);
				output_record.setDouble("職2", extract_train_jd.認可數 / extract_train_jd.記錄數);
				output_record.setDouble("職1_1", extract_train_jd.投遞數 / (1 + extract_train_jd.記錄數));
				output_record.setDouble("職2_1", extract_train_jd.認可數 / (1 + extract_train_jd.記錄數));

				extract_train_jd = 特征職位標題_hashmap.get(extract_jd.職位標題);
				output_record.setDouble("職0_1", extract_train_jd.認可數 / extract_train_jd.記錄數);
				output_record.setDouble("職0_5", extract_train_jd.認可數);

				extract_train_jd = 特征職位子類_hashmap.get(extract_jd.職位子類);
				output_record.setDouble("職0_2", extract_train_jd.認可數 / extract_train_jd.記錄數);
				output_record.setDouble("職0_6", extract_train_jd.認可數);

				extract_train_jd = 特征職位城市_hashmap.get(extract_jd.職位城市);
				output_record.setDouble("職0_3", extract_train_jd.認可數 / extract_train_jd.記錄數);
				output_record.setDouble("職0_7", extract_train_jd.認可數);

				extract_train_jd = 特征職位描述_hashmap.get(extract_jd.職位描述);
				output_record.setDouble("職0_4", extract_train_jd.認可數 / extract_train_jd.記錄數);
				output_record.setDouble("職0_8", extract_train_jd.認可數);

				context.write(output_record);
			}
		}
	}

	private class RgExtractTrainJd
	{
		double 記錄數;
		double 瀏覽數;
		double 投遞數;
		double 認可數;
	}

	private class RgExtractJd
	{
		String 職位標題;
		String 職位子類;
		String 職位城市;
		String 職位描述;
	}

}

package jiangli.rg.rg_extract;

class RgExtractUserReducer extends com.aliyun.odps.mapred.ReducerBase
{
	public void setup(TaskContext context) throws java.io.IOException
	{
	}

	public void reduce(com.aliyun.odps.data.Record key_record, java.util.Iterator<com.aliyun.odps.data.Record> value_records, TaskContext context) throws java.io.IOException
	{
		com.aliyun.odps.data.Record output_record = context.createOutputRecord();

		String 用戶期望城市 = key_record.getString("用戶期望城市");
		String 用戶期望行業 = key_record.getString("用戶期望行業");
		String 用戶期望職類 = key_record.getString("用戶期望職類");
		Double 用戶期望最低薪水 = key_record.getDouble("用戶期望最低薪水");
		Double 用戶期望最高薪水 = key_record.getDouble("用戶期望最高薪水");
		Double 用戶當前最低薪水 = key_record.getDouble("用戶當前最低薪水");
		Double 用戶當前最高薪水 = key_record.getDouble("用戶當前最高薪水");
		Double 用戶學歷值 = key_record.getDouble("用戶學歷值");
		Double 用戶年齡 = key_record.getDouble("用戶年齡");
		Double 用戶工作年限 = key_record.getDouble("用戶工作年限");
		output_record.setString("用戶標識", key_record.getString("用戶標識"));
		output_record.setDouble("用戶期望最低薪水", FillNull(用戶期望最低薪水, -1));
		output_record.setDouble("用戶期望最高薪水", FillNull(用戶期望最高薪水, -1));
		output_record.setDouble("用戶當前最低薪水", FillNull(用戶當前最低薪水, -1));
		output_record.setDouble("用戶當前最高薪水", FillNull(用戶當前最高薪水, -1));
		output_record.setDouble("用戶學歷值", FillNull(用戶學歷值, -1));
		output_record.setDouble("用戶年齡", FillNull(用戶年齡, -1));
		output_record.setDouble("用戶工作年限", FillNull(用戶工作年限, -1));
		output_record.setDouble("用戶工作年限差", CalculateDifference(用戶年齡, 用戶工作年限, -1));

		java.util.Vector<RgExtractRecord> rg_extract_record_vector = new java.util.Vector<RgExtractRecord>();
		java.util.Vector<Double> 職位最低薪水_vector = new java.util.Vector<Double>();
		java.util.Vector<Double> 職位工作年限_vector = new java.util.Vector<Double>();
		java.util.Vector<Double> 職位最低學歷值_vector = new java.util.Vector<Double>();
		java.util.HashMap<String, Double> 職位標識_hashmap = new java.util.HashMap<String, Double>();
		java.util.HashMap<String, Double> 職位標題_hashmap = new java.util.HashMap<String, Double>();
		java.util.HashMap<String, Double> 職位城市_hashmap = new java.util.HashMap<String, Double>();
		java.util.HashMap<String, Double> 職位子類_hashmap = new java.util.HashMap<String, Double>();
		java.util.HashMap<String, Double> 職位描述_hashmap = new java.util.HashMap<String, Double>();
		java.util.HashMap<String, java.util.HashSet<String>> 職位標題_職位標識_hashset_hashmap = new java.util.HashMap<String, java.util.HashSet<String>>();
		java.util.HashMap<String, java.util.HashSet<String>> 職位城市_職位標識_hashset_hashmap = new java.util.HashMap<String, java.util.HashSet<String>>();
		java.util.HashMap<String, java.util.HashSet<String>> 職位子類_職位標識_hashset_hashmap = new java.util.HashMap<String, java.util.HashSet<String>>();
		java.util.HashMap<String, java.util.HashSet<String>> 職位描述_職位標識_hashset_hashmap = new java.util.HashMap<String, java.util.HashSet<String>>();
		while (value_records.hasNext())
		{
			com.aliyun.odps.data.Record value_record = value_records.next();

			RgExtractRecord rg_extract_record = new RgExtractRecord();
			rg_extract_record.職位標識 = value_record.getString("職位標識");
			rg_extract_record.記錄標識 = value_record.getBigint("記錄標識");
			rg_extract_record.瀏覽 = value_record.getBigint("瀏覽");
			rg_extract_record.投遞 = value_record.getBigint("投遞");
			rg_extract_record.認可 = value_record.getBigint("認可");
			rg_extract_record.職位標題 = value_record.getString("職位標題");
			rg_extract_record.職位城市 = value_record.getString("職位城市");
			rg_extract_record.職位子類 = value_record.getString("職位子類");
			rg_extract_record.職位需求人數 = value_record.getDouble("職位需求人數");
			rg_extract_record.職位最低薪水 = value_record.getDouble("職位最低薪水");
			rg_extract_record.職位最高薪水 = value_record.getDouble("職位最高薪水");
			rg_extract_record.職位開始日序 = value_record.getDouble("職位開始日序");
			rg_extract_record.職位終止日序 = value_record.getDouble("職位終止日序");
			rg_extract_record.職位出差否 = value_record.getDouble("職位出差否");
			rg_extract_record.職位工作年限 = value_record.getDouble("職位工作年限");
			rg_extract_record.職位關鍵詞 = value_record.getString("職位關鍵詞");
			rg_extract_record.職位最低學歷值 = value_record.getDouble("職位最低學歷值");
			rg_extract_record.職位管理經驗 = value_record.getString("職位管理經驗");
			rg_extract_record.職位語言需求 = value_record.getString("職位語言需求");
			rg_extract_record.職位描述 = value_record.getString("職位描述");
			rg_extract_record.職0 = value_record.getDouble("職0");
			rg_extract_record.職0_1 = value_record.getDouble("職0_1");
			rg_extract_record.職0_2 = value_record.getDouble("職0_2");
			rg_extract_record.職0_3 = value_record.getDouble("職0_3");
			rg_extract_record.職0_4 = value_record.getDouble("職0_4");
			rg_extract_record.職0_5 = value_record.getDouble("職0_5");
			rg_extract_record.職0_6 = value_record.getDouble("職0_6");
			rg_extract_record.職0_7 = value_record.getDouble("職0_7");
			rg_extract_record.職0_8 = value_record.getDouble("職0_8");
			rg_extract_record.職1 = value_record.getDouble("職1");
			rg_extract_record.職2 = value_record.getDouble("職2");
			rg_extract_record.職1_1 = value_record.getDouble("職1_1");
			rg_extract_record.職2_1 = value_record.getDouble("職2_1");
			rg_extract_record.職3 = value_record.getDouble("職3");
			rg_extract_record.職4 = value_record.getDouble("職4");
			rg_extract_record.職5 = value_record.getDouble("職5");
			rg_extract_record.職6 = value_record.getDouble("職6");
			rg_extract_record.職7 = value_record.getDouble("職7");
			rg_extract_record.職8 = value_record.getDouble("職8");
			rg_extract_record.職9 = value_record.getDouble("職9");
			rg_extract_record.職10 = value_record.getDouble("職10");
			rg_extract_record.職11 = value_record.getDouble("職11");
			rg_extract_record.職12 = value_record.getDouble("職12");
			rg_extract_record.職13 = value_record.getDouble("職13");
			rg_extract_record.職14 = value_record.getDouble("職14");
			rg_extract_record.職15 = value_record.getDouble("職15");
			rg_extract_record.職16 = value_record.getDouble("職16");
			rg_extract_record.職17 = value_record.getDouble("職17");
			rg_extract_record.職18 = value_record.getDouble("職18");
			rg_extract_record.職19 = value_record.getDouble("職19");
			rg_extract_record.職20 = value_record.getDouble("職20");
		rg_extract_record.職21 = value_record.getDouble("職21");
			rg_extract_record.職22 = value_record.getDouble("職22");
			rg_extract_record.職甲0 = value_record.getDouble("職甲0");
			rg_extract_record.職甲1 = value_record.getDouble("職甲1");
			rg_extract_record.職甲2 = value_record.getDouble("職甲2");
			rg_extract_record.職甲3 = value_record.getDouble("職甲3");
			rg_extract_record.職甲4 = value_record.getDouble("職甲4");
			rg_extract_record.職甲5 = value_record.getDouble("職甲5");
			rg_extract_record.職甲6 = value_record.getDouble("職甲6");
			rg_extract_record.職甲7 = value_record.getDouble("職甲7");
			rg_extract_record.職甲8 = value_record.getDouble("職甲8");
			rg_extract_record.職甲9 = value_record.getDouble("職甲9");
			rg_extract_record.職甲10 = value_record.getDouble("職甲10");
			rg_extract_record.職甲11 = value_record.getDouble("職甲11");
			rg_extract_record.職甲12 = value_record.getDouble("職甲12");
			rg_extract_record.職甲13 = value_record.getDouble("職甲13");
			rg_extract_record.職甲14 = value_record.getDouble("職甲14");
			rg_extract_record.職甲15 = value_record.getDouble("職甲15");
			rg_extract_record.職甲16 = value_record.getDouble("職甲16");
			rg_extract_record.職甲17 = value_record.getDouble("職甲17");
			rg_extract_record.職甲18 = value_record.getDouble("職甲18");
			rg_extract_record.職甲19 = value_record.getDouble("職甲19");
			rg_extract_record.職甲20 = value_record.getDouble("職甲20");
			rg_extract_record.職甲256 = value_record.getDouble("職甲256");
			rg_extract_record.職甲257 = value_record.getDouble("職甲257");
			rg_extract_record.職乙0 = value_record.getDouble("職乙0");
			rg_extract_record.職乙1 = value_record.getDouble("職乙1");
			rg_extract_record.職乙2 = value_record.getDouble("職乙2");
			rg_extract_record.職乙3 = value_record.getDouble("職乙3");
			rg_extract_record.職乙4 = value_record.getDouble("職乙4");
			rg_extract_record.職乙5 = value_record.getDouble("職乙5");
			rg_extract_record.職乙6 = value_record.getDouble("職乙6");
			rg_extract_record.職乙7 = value_record.getDouble("職乙7");
			rg_extract_record.職乙8 = value_record.getDouble("職乙8");
			rg_extract_record.職乙9 = value_record.getDouble("職乙9");
			rg_extract_record.職乙10 = value_record.getDouble("職乙10");
			rg_extract_record.職乙11 = value_record.getDouble("職乙11");
			rg_extract_record.職乙12 = value_record.getDouble("職乙12");
			rg_extract_record.職乙13 = value_record.getDouble("職乙13");
			rg_extract_record.職乙14 = value_record.getDouble("職乙14");
			rg_extract_record.職乙15 = value_record.getDouble("職乙15");
			rg_extract_record.職乙16 = value_record.getDouble("職乙16");
			rg_extract_record.職乙17 = value_record.getDouble("職乙17");
			rg_extract_record.職乙18 = value_record.getDouble("職乙18");
			rg_extract_record.職乙19 = value_record.getDouble("職乙19");
			rg_extract_record.職乙20 = value_record.getDouble("職乙20");
			rg_extract_record.職乙256 = value_record.getDouble("職乙256");
			rg_extract_record.職乙257 = value_record.getDouble("職乙257");
			rg_extract_record.職丙0 = value_record.getDouble("職丙0");
			rg_extract_record.職丙1 = value_record.getDouble("職丙1");
			rg_extract_record.職丙2 = value_record.getDouble("職丙2");
			rg_extract_record.職丙3 = value_record.getDouble("職丙3");
			rg_extract_record.職丙4 = value_record.getDouble("職丙4");
			rg_extract_record.職丙5 = value_record.getDouble("職丙5");
			rg_extract_record.職丙6 = value_record.getDouble("職丙6");
			rg_extract_record.職丙7 = value_record.getDouble("職丙7");
			rg_extract_record.職丙8 = value_record.getDouble("職丙8");
			rg_extract_record.職丙9 = value_record.getDouble("職丙9");
			rg_extract_record.職丙10 = value_record.getDouble("職丙10");
			rg_extract_record.職丙11 = value_record.getDouble("職丙11");
			rg_extract_record.職丙12 = value_record.getDouble("職丙12");
			rg_extract_record.職丙13 = value_record.getDouble("職丙13");
			rg_extract_record.職丙14 = value_record.getDouble("職丙14");
			rg_extract_record.職丙15 = value_record.getDouble("職丙15");
			rg_extract_record.職丙16 = value_record.getDouble("職丙16");
			rg_extract_record.職丙17 = value_record.getDouble("職丙17");
			rg_extract_record.職丙18 = value_record.getDouble("職丙18");
			rg_extract_record.職丙19 = value_record.getDouble("職丙19");
			rg_extract_record.職丙20 = value_record.getDouble("職丙20");
			rg_extract_record.職丙256 = value_record.getDouble("職丙256");
			rg_extract_record.職丙257 = value_record.getDouble("職丙257");
			rg_extract_record.職丁0 = value_record.getDouble("職丁0");
			rg_extract_record.職丁1 = value_record.getDouble("職丁1");
			rg_extract_record.職丁2 = value_record.getDouble("職丁2");
			rg_extract_record.職丁3 = value_record.getDouble("職丁3");
			rg_extract_record.職丁4 = value_record.getDouble("職丁4");
			rg_extract_record.職丁5 = value_record.getDouble("職丁5");
			rg_extract_record.職丁6 = value_record.getDouble("職丁6");
			rg_extract_record.職丁7 = value_record.getDouble("職丁7");
			rg_extract_record.職丁8 = value_record.getDouble("職丁8");
			rg_extract_record.職丁9 = value_record.getDouble("職丁9");
			rg_extract_record.職丁10 = value_record.getDouble("職丁10");
			rg_extract_record.職丁11 = value_record.getDouble("職丁11");
			rg_extract_record.職丁12 = value_record.getDouble("職丁12");
			rg_extract_record.職丁13 = value_record.getDouble("職丁13");
			rg_extract_record.職丁14 = value_record.getDouble("職丁14");
			rg_extract_record.職丁15 = value_record.getDouble("職丁15");
			rg_extract_record.職丁16 = value_record.getDouble("職丁16");
			rg_extract_record.職丁17 = value_record.getDouble("職丁17");
			rg_extract_record.職丁18 = value_record.getDouble("職丁18");
			rg_extract_record.職丁19 = value_record.getDouble("職丁19");
			rg_extract_record.職丁20 = value_record.getDouble("職丁20");
			rg_extract_record.職丁256 = value_record.getDouble("職丁256");
			rg_extract_record.職丁257 = value_record.getDouble("職丁257");
			rg_extract_record.職戊256 = value_record.getDouble("職戊256");
			rg_extract_record.職戊257 = value_record.getDouble("職戊257");

			rg_extract_record_vector.add(rg_extract_record);
			職位最低薪水_vector.add(rg_extract_record.職位最低薪水);
			職位工作年限_vector.add(rg_extract_record.職位工作年限);
			職位最低學歷值_vector.add(rg_extract_record.職位最低學歷值);
			職位標識_hashmap.put(rg_extract_record.職位標識, 1 + 職位標識_hashmap.getOrDefault(rg_extract_record.職位標識, 0D));

			if (rg_extract_record.職位標題 != null)
			{
				職位標題_hashmap.put(rg_extract_record.職位標題, 1 + 職位標題_hashmap.getOrDefault(rg_extract_record.職位標題, 0D));
				if (!職位標題_職位標識_hashset_hashmap.containsKey(rg_extract_record.職位標題))
					職位標題_職位標識_hashset_hashmap.put(rg_extract_record.職位標題, new java.util.HashSet<String>());
				職位標題_職位標識_hashset_hashmap.get(rg_extract_record.職位標題).add(rg_extract_record.職位標識);
			}
			if (rg_extract_record.職位城市 != null)
			{
				職位城市_hashmap.put(rg_extract_record.職位城市, 1 + 職位城市_hashmap.getOrDefault(rg_extract_record.職位城市, 0D));
				if (!職位城市_職位標識_hashset_hashmap.containsKey(rg_extract_record.職位城市))
					職位城市_職位標識_hashset_hashmap.put(rg_extract_record.職位城市, new java.util.HashSet<String>());
				職位城市_職位標識_hashset_hashmap.get(rg_extract_record.職位城市).add(rg_extract_record.職位標識);
			}
			if (rg_extract_record.職位子類 != null)
			{
				職位子類_hashmap.put(rg_extract_record.職位子類, 1 + 職位子類_hashmap.getOrDefault(rg_extract_record.職位子類, 0D));
				if (!職位子類_職位標識_hashset_hashmap.containsKey(rg_extract_record.職位子類))
					職位子類_職位標識_hashset_hashmap.put(rg_extract_record.職位子類, new java.util.HashSet<String>());
				職位子類_職位標識_hashset_hashmap.get(rg_extract_record.職位子類).add(rg_extract_record.職位標識);
			}
			if (rg_extract_record.職位描述 != null)
			{
				職位描述_hashmap.put(rg_extract_record.職位描述, 1 + 職位描述_hashmap.getOrDefault(rg_extract_record.職位描述, 0D));
				if (!職位描述_職位標識_hashset_hashmap.containsKey(rg_extract_record.職位描述))
					職位描述_職位標識_hashset_hashmap.put(rg_extract_record.職位描述, new java.util.HashSet<String>());
				職位描述_職位標識_hashset_hashmap.get(rg_extract_record.職位描述).add(rg_extract_record.職位標識);
			}
		}

		rg_extract_record_vector.sort((RgExtractRecord a, RgExtractRecord b) -> Long.compare(a.記錄標識, b.記錄標識));
		for (int i = 0; i < rg_extract_record_vector.size(); i++)
		{
			RgExtractRecord rg_extract_record = rg_extract_record_vector.get(i);
			if (i >= 1)
				rg_extract_record.前職位標識 = rg_extract_record_vector.get(i - 1).職位標識;
			if (i < rg_extract_record_vector.size() - 1)
				rg_extract_record.後職位標識 = rg_extract_record_vector.get(1 + i).職位標識;
		}



		java.util.HashMap<String, Double> 職位標識序號_hashmap = new java.util.HashMap<String, Double>();
		java.util.HashMap<String, Double> 職位子類序號_hashmap = new java.util.HashMap<String, Double>();
		java.util.HashMap<String, Double> 職位城市序號_hashmap = new java.util.HashMap<String, Double>();
		java.util.HashMap<String, Double> 職位描述序號_hashmap = new java.util.HashMap<String, Double>();
		java.util.HashMap<String, Double> 職位標題序號_hashmap = new java.util.HashMap<String, Double>();
		java.util.HashMap<String, Double> 前職位標識序號_hashmap = new java.util.HashMap<String, Double>();
		java.util.HashMap<String, Double> 後職位標識序號_hashmap = new java.util.HashMap<String, Double>();
		java.util.HashMap<String, Double> 職位標識組序號_hashmap = new java.util.HashMap<String, Double>();
		java.util.HashMap<String, Double> 職位子類組序號_hashmap = new java.util.HashMap<String, Double>();
		java.util.HashMap<String, Double> 職位城市組序號_hashmap = new java.util.HashMap<String, Double>();
		java.util.HashMap<String, Double> 職位描述組序號_hashmap = new java.util.HashMap<String, Double>();
		java.util.HashMap<String, Double> 職位標題組序號_hashmap = new java.util.HashMap<String, Double>();
		java.util.HashMap<String, Double> 前職位標識組序號_hashmap = new java.util.HashMap<String, Double>();
		java.util.HashMap<String, Double> 後職位標識組序號_hashmap = new java.util.HashMap<String, Double>();
		java.util.HashMap<Long, Double> 記錄職位標識前距離_hashmap = new java.util.HashMap<Long, Double>();
		java.util.HashMap<Long, Double> 記錄職位子類前距離_hashmap = new java.util.HashMap<Long, Double>();
		java.util.HashMap<Long, Double> 記錄職位城市前距離_hashmap = new java.util.HashMap<Long, Double>();
		java.util.HashMap<Long, Double> 記錄職位描述前距離_hashmap = new java.util.HashMap<Long, Double>();
		java.util.HashMap<Long, Double> 記錄職位標題前距離_hashmap = new java.util.HashMap<Long, Double>();
		java.util.HashMap<Long, Double> 記錄前職位標識前距離_hashmap = new java.util.HashMap<Long, Double>();
		java.util.HashMap<Long, Double> 記錄後職位標識前距離_hashmap = new java.util.HashMap<Long, Double>();
		java.util.HashMap<Long, Double> 記錄職位標識序號_hashmap = new java.util.HashMap<Long, Double>();
		java.util.HashMap<Long, Double> 記錄職位子類序號_hashmap = new java.util.HashMap<Long, Double>();
		java.util.HashMap<Long, Double> 記錄職位城市序號_hashmap = new java.util.HashMap<Long, Double>();
		java.util.HashMap<Long, Double> 記錄職位描述序號_hashmap = new java.util.HashMap<Long, Double>();
		java.util.HashMap<Long, Double> 記錄職位標題序號_hashmap = new java.util.HashMap<Long, Double>();
		java.util.HashMap<Long, Double> 記錄前職位標識序號_hashmap = new java.util.HashMap<Long, Double>();
		java.util.HashMap<Long, Double> 記錄後職位標識序號_hashmap = new java.util.HashMap<Long, Double>();
		for (int i = 0; i < rg_extract_record_vector.size(); i++)
		{
			RgExtractRecord rg_extract_record = rg_extract_record_vector.get(i);
			String 職位標識 = rg_extract_record.職位標識;
			String 職位子類 = rg_extract_record.職位子類;
			String 職位城市 = rg_extract_record.職位城市;
			String 職位描述 = rg_extract_record.職位描述;
			String 職位標題 = rg_extract_record.職位標題;
			String 前職位標識 = rg_extract_record.前職位標識;
			String 後職位標識 = rg_extract_record.後職位標識;
			long 記錄標識 = rg_extract_record.記錄標識;
			double 序號 = 1 + i;

			if (!職位標識序號_hashmap.containsKey(職位標識))
			{
				記錄職位標識前距離_hashmap.put(記錄標識, null);
				記錄職位標識序號_hashmap.put(記錄標識, 0D);
				職位標識組序號_hashmap.put(職位標識, 0D);
			}
			else
			{
				記錄職位標識前距離_hashmap.put(記錄標識, 序號 - 職位標識序號_hashmap.get(職位標識));
				記錄職位標識序號_hashmap.put(記錄標識, 職位標識組序號_hashmap.get(職位標識));
			}
			職位標識序號_hashmap.put(職位標識, 序號);
			職位標識組序號_hashmap.put(職位標識, 1 + 職位標識組序號_hashmap.get(職位標識));

			if (!職位子類序號_hashmap.containsKey(職位子類))
			{
				記錄職位子類前距離_hashmap.put(記錄標識, null);
				記錄職位子類序號_hashmap.put(記錄標識, 0D);
				職位子類組序號_hashmap.put(職位子類, 0D);
			}
			else
			{
				記錄職位子類前距離_hashmap.put(記錄標識, 序號 - 職位子類序號_hashmap.get(職位子類));
				記錄職位子類序號_hashmap.put(記錄標識, 職位子類組序號_hashmap.get(職位子類));
			}
			職位子類序號_hashmap.put(職位子類, 序號);
			職位子類組序號_hashmap.put(職位子類, 1 + 職位子類組序號_hashmap.get(職位子類));

			if (!職位城市序號_hashmap.containsKey(職位城市))
			{
				記錄職位城市前距離_hashmap.put(記錄標識, null);
				記錄職位城市序號_hashmap.put(記錄標識, 0D);
				職位城市組序號_hashmap.put(職位城市, 0D);
			}
			else
			{
				記錄職位城市前距離_hashmap.put(記錄標識, 序號 - 職位城市序號_hashmap.get(職位城市));
				記錄職位城市序號_hashmap.put(記錄標識, 職位城市組序號_hashmap.get(職位城市));
			}
			職位城市序號_hashmap.put(職位城市, 序號);
			職位城市組序號_hashmap.put(職位城市, 1 + 職位城市組序號_hashmap.get(職位城市));

			if (!職位描述序號_hashmap.containsKey(職位描述))
			{
				記錄職位描述前距離_hashmap.put(記錄標識, null);
				記錄職位描述序號_hashmap.put(記錄標識, 0D);
				職位描述組序號_hashmap.put(職位描述, 0D);
			}
			else
			{
				記錄職位描述前距離_hashmap.put(記錄標識, 序號 - 職位描述序號_hashmap.get(職位描述));
				記錄職位描述序號_hashmap.put(記錄標識, 職位描述組序號_hashmap.get(職位描述));
			}
			職位描述序號_hashmap.put(職位描述, 序號);
			職位描述組序號_hashmap.put(職位描述, 1 + 職位描述組序號_hashmap.get(職位描述));

			if (!職位標題序號_hashmap.containsKey(職位標題))
			{
				記錄職位標題前距離_hashmap.put(記錄標識, null);
				記錄職位標題序號_hashmap.put(記錄標識, 0D);
				職位標題組序號_hashmap.put(職位標題, 0D);
			}
			else
			{
				記錄職位標題前距離_hashmap.put(記錄標識, 序號 - 職位標題序號_hashmap.get(職位標題));
				記錄職位標題序號_hashmap.put(記錄標識, 職位標題組序號_hashmap.get(職位標題));
			}
			職位標題序號_hashmap.put(職位標題, 序號);
			職位標題組序號_hashmap.put(職位標題, 1 + 職位標題組序號_hashmap.get(職位標題));

			if (!前職位標識序號_hashmap.containsKey(前職位標識))
			{
				記錄前職位標識前距離_hashmap.put(記錄標識, null);
				記錄前職位標識序號_hashmap.put(記錄標識, 0D);
				前職位標識組序號_hashmap.put(前職位標識, 0D);
			}
			else
			{
				記錄前職位標識前距離_hashmap.put(記錄標識, 序號 - 前職位標識序號_hashmap.get(前職位標識));
				記錄前職位標識序號_hashmap.put(記錄標識, 前職位標識組序號_hashmap.get(前職位標識));
			}
			前職位標識序號_hashmap.put(前職位標識, 序號);
			前職位標識組序號_hashmap.put(前職位標識, 1 + 前職位標識組序號_hashmap.get(前職位標識));

			if (!後職位標識序號_hashmap.containsKey(後職位標識))
			{
				記錄後職位標識前距離_hashmap.put(記錄標識, null);
				記錄後職位標識序號_hashmap.put(記錄標識, 0D);
				後職位標識組序號_hashmap.put(後職位標識, 0D);
			}
			else
			{
				記錄後職位標識前距離_hashmap.put(記錄標識, 序號 - 後職位標識序號_hashmap.get(後職位標識));
				記錄後職位標識序號_hashmap.put(記錄標識, 後職位標識組序號_hashmap.get(後職位標識));
			}
			後職位標識序號_hashmap.put(後職位標識, 序號);
			後職位標識組序號_hashmap.put(後職位標識, 1 + 後職位標識組序號_hashmap.get(後職位標識));
		}

		java.util.HashMap<String, Double> 職位標識逆序號_hashmap = new java.util.HashMap<String, Double>();
		java.util.HashMap<String, Double> 職位子類逆序號_hashmap = new java.util.HashMap<String, Double>();
		java.util.HashMap<String, Double> 職位城市逆序號_hashmap = new java.util.HashMap<String, Double>();
		java.util.HashMap<String, Double> 職位描述逆序號_hashmap = new java.util.HashMap<String, Double>();
		java.util.HashMap<String, Double> 職位標題逆序號_hashmap = new java.util.HashMap<String, Double>();
		java.util.HashMap<String, Double> 前職位標識逆序號_hashmap = new java.util.HashMap<String, Double>();
		java.util.HashMap<String, Double> 後職位標識逆序號_hashmap = new java.util.HashMap<String, Double>();
		java.util.HashMap<String, Double> 職位標識組逆序號_hashmap = new java.util.HashMap<String, Double>();
		java.util.HashMap<String, Double> 職位子類組逆序號_hashmap = new java.util.HashMap<String, Double>();
		java.util.HashMap<String, Double> 職位城市組逆序號_hashmap = new java.util.HashMap<String, Double>();
		java.util.HashMap<String, Double> 職位描述組逆序號_hashmap = new java.util.HashMap<String, Double>();
		java.util.HashMap<String, Double> 職位標題組逆序號_hashmap = new java.util.HashMap<String, Double>();
		java.util.HashMap<String, Double> 前職位標識組逆序號_hashmap = new java.util.HashMap<String, Double>();
		java.util.HashMap<String, Double> 後職位標識組逆序號_hashmap = new java.util.HashMap<String, Double>();
		java.util.HashMap<Long, Double> 記錄職位標識後距離_hashmap = new java.util.HashMap<Long, Double>();
		java.util.HashMap<Long, Double> 記錄職位子類後距離_hashmap = new java.util.HashMap<Long, Double>();
		java.util.HashMap<Long, Double> 記錄職位城市後距離_hashmap = new java.util.HashMap<Long, Double>();
		java.util.HashMap<Long, Double> 記錄職位描述後距離_hashmap = new java.util.HashMap<Long, Double>();
		java.util.HashMap<Long, Double> 記錄職位標題後距離_hashmap = new java.util.HashMap<Long, Double>();
		java.util.HashMap<Long, Double> 記錄前職位標識後距離_hashmap = new java.util.HashMap<Long, Double>();
		java.util.HashMap<Long, Double> 記錄後職位標識後距離_hashmap = new java.util.HashMap<Long, Double>();
		java.util.HashMap<Long, Double> 記錄職位標識逆序號_hashmap = new java.util.HashMap<Long, Double>();
		java.util.HashMap<Long, Double> 記錄職位子類逆序號_hashmap = new java.util.HashMap<Long, Double>();
		java.util.HashMap<Long, Double> 記錄職位城市逆序號_hashmap = new java.util.HashMap<Long, Double>();
		java.util.HashMap<Long, Double> 記錄職位描述逆序號_hashmap = new java.util.HashMap<Long, Double>();
		java.util.HashMap<Long, Double> 記錄職位標題逆序號_hashmap = new java.util.HashMap<Long, Double>();
		java.util.HashMap<Long, Double> 記錄前職位標識逆序號_hashmap = new java.util.HashMap<Long, Double>();
		java.util.HashMap<Long, Double> 記錄後職位標識逆序號_hashmap = new java.util.HashMap<Long, Double>();
		for (int i = rg_extract_record_vector.size() - 1; i >= 0; i--)
		{
			RgExtractRecord rg_extract_record = rg_extract_record_vector.get(i);
			String 職位標識 = rg_extract_record.職位標識;
			String 職位子類 = rg_extract_record.職位子類;
			String 職位城市 = rg_extract_record.職位城市;
			String 職位描述 = rg_extract_record.職位描述;
			String 職位標題 = rg_extract_record.職位標題;
			String 前職位標識 = rg_extract_record.前職位標識;
			String 後職位標識 = rg_extract_record.後職位標識;
			long 記錄標識 = rg_extract_record.記錄標識;
			double 逆序號 = rg_extract_record_vector.size() - i;

			if (!職位標識逆序號_hashmap.containsKey(職位標識))
			{
				記錄職位標識後距離_hashmap.put(記錄標識, null);
				記錄職位標識逆序號_hashmap.put(記錄標識, 0D);
				職位標識組逆序號_hashmap.put(職位標識, 0D);
			}
			else
			{
				記錄職位標識後距離_hashmap.put(記錄標識, 逆序號 - 職位標識逆序號_hashmap.get(職位標識));
				記錄職位標識逆序號_hashmap.put(記錄標識, 職位標識組逆序號_hashmap.get(職位標識));

			}
			職位標識逆序號_hashmap.put(職位標識, 逆序號);
			職位標識組逆序號_hashmap.put(職位標識, 1 + 職位標識組逆序號_hashmap.get(職位標識));

			if (!職位子類逆序號_hashmap.containsKey(職位子類))
			{
				記錄職位子類後距離_hashmap.put(記錄標識, null);
				記錄職位子類逆序號_hashmap.put(記錄標識, 0D);
				職位子類組逆序號_hashmap.put(職位子類, 0D);
			}
			else
			{
				記錄職位子類後距離_hashmap.put(記錄標識, 逆序號 - 職位子類逆序號_hashmap.get(職位子類));
				記錄職位子類逆序號_hashmap.put(記錄標識, 職位子類組逆序號_hashmap.get(職位子類));

			}
			職位子類逆序號_hashmap.put(職位子類, 逆序號);
			職位子類組逆序號_hashmap.put(職位子類, 1 + 職位子類組逆序號_hashmap.get(職位子類));

			if (!職位城市逆序號_hashmap.containsKey(職位城市))
			{
				記錄職位城市後距離_hashmap.put(記錄標識, null);
				記錄職位城市逆序號_hashmap.put(記錄標識, 0D);
				職位城市組逆序號_hashmap.put(職位城市, 0D);
			}
			else
			{
				記錄職位城市後距離_hashmap.put(記錄標識, 逆序號 - 職位城市逆序號_hashmap.get(職位城市));
				記錄職位城市逆序號_hashmap.put(記錄標識, 職位城市組逆序號_hashmap.get(職位城市));

			}
			職位城市逆序號_hashmap.put(職位城市, 逆序號);
			職位城市組逆序號_hashmap.put(職位城市, 1 + 職位城市組逆序號_hashmap.get(職位城市));

			if (!職位描述逆序號_hashmap.containsKey(職位描述))
			{
				記錄職位描述後距離_hashmap.put(記錄標識, null);
				記錄職位描述逆序號_hashmap.put(記錄標識, 0D);
				職位描述組逆序號_hashmap.put(職位描述, 0D);
			}
			else
			{
				記錄職位描述後距離_hashmap.put(記錄標識, 逆序號 - 職位描述逆序號_hashmap.get(職位描述));
				記錄職位描述逆序號_hashmap.put(記錄標識, 職位描述組逆序號_hashmap.get(職位描述));

			}
			職位描述逆序號_hashmap.put(職位描述, 逆序號);
			職位描述組逆序號_hashmap.put(職位描述, 1 + 職位描述組逆序號_hashmap.get(職位描述));

			if (!職位標題逆序號_hashmap.containsKey(職位標題))
			{
				記錄職位標題後距離_hashmap.put(記錄標識, null);
				記錄職位標題逆序號_hashmap.put(記錄標識, 0D);
				職位標題組逆序號_hashmap.put(職位標題, 0D);
			}
			else
			{
				記錄職位標題後距離_hashmap.put(記錄標識, 逆序號 - 職位標題逆序號_hashmap.get(職位標題));
				記錄職位標題逆序號_hashmap.put(記錄標識, 職位標題組逆序號_hashmap.get(職位標題));

			}
			職位標題逆序號_hashmap.put(職位標題, 逆序號);
			職位標題組逆序號_hashmap.put(職位標題, 1 + 職位標題組逆序號_hashmap.get(職位標題));

			if (!前職位標識逆序號_hashmap.containsKey(前職位標識))
			{
				記錄前職位標識後距離_hashmap.put(記錄標識, null);
				記錄前職位標識逆序號_hashmap.put(記錄標識, 0D);
				前職位標識組逆序號_hashmap.put(前職位標識, 0D);
			}
			else
			{
				記錄前職位標識後距離_hashmap.put(記錄標識, 逆序號 - 前職位標識逆序號_hashmap.get(前職位標識));
				記錄前職位標識逆序號_hashmap.put(記錄標識, 前職位標識組逆序號_hashmap.get(前職位標識));

			}
			前職位標識逆序號_hashmap.put(前職位標識, 逆序號);
			前職位標識組逆序號_hashmap.put(前職位標識, 1 + 前職位標識組逆序號_hashmap.get(前職位標識));

			if (!後職位標識逆序號_hashmap.containsKey(後職位標識))
			{
				記錄後職位標識後距離_hashmap.put(記錄標識, null);
				記錄後職位標識逆序號_hashmap.put(記錄標識, 0D);
				後職位標識組逆序號_hashmap.put(後職位標識, 0D);
			}
			else
			{
				記錄後職位標識後距離_hashmap.put(記錄標識, 逆序號 - 後職位標識逆序號_hashmap.get(後職位標識));
				記錄後職位標識逆序號_hashmap.put(記錄標識, 後職位標識組逆序號_hashmap.get(後職位標識));

			}
			後職位標識逆序號_hashmap.put(後職位標識, 逆序號);
			後職位標識組逆序號_hashmap.put(後職位標識, 1 + 後職位標識組逆序號_hashmap.get(後職位標識));
		}

		for (int i = 0; i < rg_extract_record_vector.size(); i++)
		{
			RgExtractRecord rg_extract_record = rg_extract_record_vector.get(i);
			String 職位標識 = rg_extract_record.職位標識;
			long 記錄標識 = rg_extract_record.記錄標識;
			double 序號 = 1 + i;
			double 逆序號 = rg_extract_record_vector.size() - i;
			output_record.setString("職位標識", 職位標識);
			output_record.setBigint("記錄標識", 記錄標識);
			output_record.setBigint("瀏覽", rg_extract_record.瀏覽);
			output_record.setBigint("投遞", rg_extract_record.投遞);
			output_record.setBigint("認可", rg_extract_record.認可);
			output_record.setDouble("特0", 序號);
			output_record.setDouble("特1", 逆序號);
			output_record.setDouble("職位需求人數", FillNull(rg_extract_record.職位需求人數, -1));
			output_record.setDouble("職位最低薪水", FillNull(rg_extract_record.職位最低薪水, -1));
			output_record.setDouble("職位最高薪水", FillNull(rg_extract_record.職位最高薪水, -1));
			output_record.setDouble("職位出差否", FillNull(rg_extract_record.職位出差否, -1));
			output_record.setDouble("職位工作年限", FillNull(rg_extract_record.職位工作年限, -1));
			output_record.setDouble("職位最低學歷值", FillNull(rg_extract_record.職位最低學歷值, -1));
			output_record.setDouble("職位開始日序", FillNull(rg_extract_record.職位開始日序, -1));
			output_record.setDouble("職位終止日序", FillNull(rg_extract_record.職位終止日序, -1));
			output_record.setDouble("特2", CalculateDifference(用戶期望最低薪水, rg_extract_record.職位最低薪水, 0));
			output_record.setDouble("特3", CalculateDifference(用戶期望最低薪水, rg_extract_record.職位最高薪水, 0));
			output_record.setDouble("特4", CalculateDifference(rg_extract_record.職位終止日序, rg_extract_record.職位開始日序, -1));
			output_record.setDouble("特5", CalculateDifference(用戶學歷值, rg_extract_record.職位最低學歷值, 0));

			Double 特6 = GetMedian(職位最低薪水_vector, null);
			Double 特7 = GetMedian(職位工作年限_vector, null);
			Double 特8 = GetMedian(職位最低學歷值_vector, null);
			output_record.setDouble("特6", FillNull(特6, -1));
			output_record.setDouble("特7", FillNull(特7, -1));
			output_record.setDouble("特8", FillNull(特8, -1));
			output_record.setDouble("特9", CalculateQuotient(特6, rg_extract_record.職位最低薪水, -1));
			output_record.setDouble("特10", CalculateQuotient(特7, rg_extract_record.職位工作年限, -1));
			output_record.setDouble("特11", CalculateQuotient(特8, rg_extract_record.職位最低學歷值, -1));
			output_record.setDouble("特12", JudgeCity(rg_extract_record.職位城市, 用戶期望城市));

			Double 特13 = rg_extract_record.職位標題 == null ? null : (double)rg_extract_record.職位標題.length();
			Double 特14 = rg_extract_record.職位描述 == null ? null : (double)rg_extract_record.職位描述.length();
			output_record.setDouble("特13", FillNull(特13, -1));
			output_record.setDouble("特14", FillNull(特14, -1));
			output_record.setDouble("特15", CalculateDifference(特14, 特13, -1));
			output_record.setDouble("特16", CalculateStringMatchingScore(用戶期望行業, rg_extract_record.職位描述));
			output_record.setDouble("特17", CalculateStringMatchingScore(用戶期望職類, rg_extract_record.職位描述));

			double 特18 = 職位標識_hashmap.getOrDefault(rg_extract_record.職位標識, 0D);
			Double 特19 = rg_extract_record.職位標題 == null ? null : 職位標題_hashmap.getOrDefault(rg_extract_record.職位標題, 0D);
			Double 特20 = rg_extract_record.職位城市 == null ? null : 職位城市_hashmap.getOrDefault(rg_extract_record.職位城市, 0D);
			Double 特21 = rg_extract_record.職位子類 == null ? null : 職位子類_hashmap.getOrDefault(rg_extract_record.職位子類, 0D);
			Double 特22 = rg_extract_record.職位描述 == null ? null : 職位描述_hashmap.getOrDefault(rg_extract_record.職位描述, 0D);
			Double 特19_1 = rg_extract_record.職位標題 == null ? null : (double)職位標題_職位標識_hashset_hashmap.getOrDefault(rg_extract_record.職位標題, new java.util.HashSet<String>()).size();
			Double 特20_1 = rg_extract_record.職位城市 == null ? null : (double)職位城市_職位標識_hashset_hashmap.getOrDefault(rg_extract_record.職位城市, new java.util.HashSet<String>()).size();
			Double 特21_1 = rg_extract_record.職位子類 == null ? null : (double)職位子類_職位標識_hashset_hashmap.getOrDefault(rg_extract_record.職位子類, new java.util.HashSet<String>()).size();
			Double 特22_1 = rg_extract_record.職位描述 == null ? null : (double)職位描述_職位標識_hashset_hashmap.getOrDefault(rg_extract_record.職位描述, new java.util.HashSet<String>()).size();
			output_record.setDouble("特18", FillNull(特18, -1));
			output_record.setDouble("特19", FillNull(特19, -1));
			output_record.setDouble("特20", FillNull(特20, -1));
			output_record.setDouble("特21", FillNull(特21, -1));
			output_record.setDouble("特22", FillNull(特22, -1));
			output_record.setDouble("特19_1", FillNull(特19_1, -1));
			output_record.setDouble("特20_1", FillNull(特20_1, -1));
			output_record.setDouble("特21_1", FillNull(特21_1, -1));
			output_record.setDouble("特22_1", FillNull(特22_1, -1));
			output_record.setDouble("特23_1", CalculateQuotient(特18, 特19, -1));
			output_record.setDouble("特23_2", CalculateQuotient(特18, 特20, -1));
			output_record.setDouble("特23_3", CalculateQuotient(特18, 特21, -1));
			output_record.setDouble("特23_4", CalculateQuotient(特18, 特22, -1));
			output_record.setDouble("特23_5", CalculateQuotient(特19, 特20, -1));
			output_record.setDouble("特23_6", CalculateQuotient(特19, 特21, -1));
			output_record.setDouble("特23_7", CalculateQuotient(特19, 特22, -1));
			output_record.setDouble("特23_8", CalculateQuotient(特20, 特21, -1));
			output_record.setDouble("特23_9", CalculateQuotient(特20, 特22, -1));
			output_record.setDouble("特23_10", CalculateQuotient(特21, 特22, -1));
			output_record.setDouble("特24_1", CalculateQuotient(特19_1, 特20_1, -1));
			output_record.setDouble("特24_2", CalculateQuotient(特19_1, 特21_1, -1));
			output_record.setDouble("特24_3", CalculateQuotient(特19_1, 特22_1, -1));
			output_record.setDouble("特24_4", CalculateQuotient(特20_1, 特21_1, -1));
			output_record.setDouble("特24_5", CalculateQuotient(特20_1, 特22_1, -1));
			output_record.setDouble("特24_6", CalculateQuotient(特21_1, 特22_1, -1));
			output_record.setDouble("特25_1", CalculateQuotient(特19_1, 特19, -1));
			output_record.setDouble("特25_2", CalculateQuotient(特20_1, 特20, -1));
			output_record.setDouble("特25_3", CalculateQuotient(特21_1, 特21, -1));
			output_record.setDouble("特25_4", CalculateQuotient(特22_1, 特22, -1));

			Double 特28 = null;
			Double 特30 = null;
			Double 特32 = null;
			Double 特34 = null;
			if (i >= 1)
			{
				特28 = 0D;
				if (rg_extract_record.職位標識.equals(rg_extract_record_vector.get(i - 1).職位標識))
					特28 = 1D;

				特30 = 職位標識_hashmap.getOrDefault(rg_extract_record_vector.get(i - 1).職位標識, 0D);
				if (i >= 2)
				{
					特32 = 職位標識_hashmap.getOrDefault(rg_extract_record_vector.get(i - 2).職位標識, 0D);
					if (i >= 3)
						特34 = 職位標識_hashmap.getOrDefault(rg_extract_record_vector.get(i - 3).職位標識, 0D);
				}
			}

			Double 特29 = null;
			Double 特31 = null;
			Double 特33 = null;
			Double 特35 = null;
			if (i < rg_extract_record_vector.size() - 1)
			{
				特29 = 0D;
				if (rg_extract_record.職位標識.equals(rg_extract_record_vector.get(1 + i).職位標識))
					特29 = 1D;

				特31 = 職位標識_hashmap.getOrDefault(rg_extract_record_vector.get(1 + i).職位標識, 0D);
				if (i < rg_extract_record_vector.size() - 2)
				{
					特33 = 職位標識_hashmap.getOrDefault(rg_extract_record_vector.get(2 + i).職位標識, 0D);
					if (i < rg_extract_record_vector.size() - 3)
						特35 = 職位標識_hashmap.getOrDefault(rg_extract_record_vector.get(3 + i).職位標識, 0D);
				}
			}
			output_record.setDouble("特28", FillNull(特28, -1));
			output_record.setDouble("特29", FillNull(特29, -1));
			output_record.setDouble("特30", FillNull(特30, -1));
			output_record.setDouble("特31", FillNull(特31, -1));
			output_record.setDouble("特32", FillNull(特32, -1));
			output_record.setDouble("特33", FillNull(特33, -1));
			output_record.setDouble("特34", FillNull(特34, -1));
			output_record.setDouble("特35", FillNull(特35, -1));

			Double 特36 = 1 + rg_extract_record_vector.size() - 職位標識逆序號_hashmap.getOrDefault(職位標識, null);
			Double 特37 = 職位標識序號_hashmap.getOrDefault(職位標識, null);
			Double 特38 = 1 + rg_extract_record_vector.size() - 職位標識序號_hashmap.getOrDefault(職位標識, null);
			Double 特39 = 職位標識逆序號_hashmap.getOrDefault(職位標識, null);
			output_record.setDouble("特36", FillNull(特36, -1));
			output_record.setDouble("特37", FillNull(特37, -1));
			output_record.setDouble("特38", FillNull(特38, -1));
			output_record.setDouble("特39", FillNull(特39, -1));
			output_record.setDouble("特40", CalculateDifference(特39, 特38, -1));
			output_record.setDouble("特41_1", FillNull(記錄職位標識前距離_hashmap.getOrDefault(記錄標識, null), -1));
			output_record.setDouble("特41_2", FillNull(記錄職位子類前距離_hashmap.getOrDefault(記錄標識, null), -1));
			output_record.setDouble("特41_3", FillNull(記錄職位城市前距離_hashmap.getOrDefault(記錄標識, null), -1));
			output_record.setDouble("特41_4", FillNull(記錄職位描述前距離_hashmap.getOrDefault(記錄標識, null), -1));
			output_record.setDouble("特41_5", FillNull(記錄職位標題前距離_hashmap.getOrDefault(記錄標識, null), -1));
			output_record.setDouble("特41_6", FillNull(記錄前職位標識前距離_hashmap.getOrDefault(記錄標識, null), -1));
			output_record.setDouble("特41_7", FillNull(記錄後職位標識前距離_hashmap.getOrDefault(記錄標識, null), -1));
			output_record.setDouble("特42_1", FillNull(記錄職位標識後距離_hashmap.getOrDefault(記錄標識, null), -1));
			output_record.setDouble("特42_2", FillNull(記錄職位子類後距離_hashmap.getOrDefault(記錄標識, null), -1));
			output_record.setDouble("特42_3", FillNull(記錄職位城市後距離_hashmap.getOrDefault(記錄標識, null), -1));
			output_record.setDouble("特42_4", FillNull(記錄職位描述後距離_hashmap.getOrDefault(記錄標識, null), -1));
			output_record.setDouble("特42_5", FillNull(記錄職位標題後距離_hashmap.getOrDefault(記錄標識, null), -1));
			output_record.setDouble("特42_6", FillNull(記錄前職位標識後距離_hashmap.getOrDefault(記錄標識, null), -1));
			output_record.setDouble("特42_7", FillNull(記錄後職位標識後距離_hashmap.getOrDefault(記錄標識, null), -1));
			output_record.setDouble("特43_1", FillNull(記錄職位標識序號_hashmap.getOrDefault(記錄標識, null), -1));
			output_record.setDouble("特43_2", FillNull(記錄職位子類序號_hashmap.getOrDefault(記錄標識, null), -1));
			output_record.setDouble("特43_3", FillNull(記錄職位城市序號_hashmap.getOrDefault(記錄標識, null), -1));
			output_record.setDouble("特43_4", FillNull(記錄職位描述序號_hashmap.getOrDefault(記錄標識, null), -1));
			output_record.setDouble("特43_5", FillNull(記錄職位標題序號_hashmap.getOrDefault(記錄標識, null), -1));
			output_record.setDouble("特43_6", FillNull(記錄前職位標識序號_hashmap.getOrDefault(記錄標識, null), -1));
			output_record.setDouble("特43_7", FillNull(記錄後職位標識序號_hashmap.getOrDefault(記錄標識, null), -1));
			output_record.setDouble("特44_1", FillNull(記錄職位標識逆序號_hashmap.getOrDefault(記錄標識, null), -1));
			output_record.setDouble("特44_2", FillNull(記錄職位子類逆序號_hashmap.getOrDefault(記錄標識, null), -1));
			output_record.setDouble("特44_3", FillNull(記錄職位城市逆序號_hashmap.getOrDefault(記錄標識, null), -1));
			output_record.setDouble("特44_4", FillNull(記錄職位描述逆序號_hashmap.getOrDefault(記錄標識, null), -1));
			output_record.setDouble("特44_5", FillNull(記錄職位標題逆序號_hashmap.getOrDefault(記錄標識, null), -1));
			output_record.setDouble("特44_6", FillNull(記錄前職位標識逆序號_hashmap.getOrDefault(記錄標識, null), -1));
			output_record.setDouble("特44_7", FillNull(記錄後職位標識逆序號_hashmap.getOrDefault(記錄標識, null), -1));

			output_record.setDouble("特57", FillNull(rg_extract_record.職0, -1));
			output_record.setDouble("特58", FillNull(rg_extract_record.職0_1, -1));
			output_record.setDouble("特59", FillNull(rg_extract_record.職0_2, -1));
			output_record.setDouble("特60", FillNull(rg_extract_record.職0_3, -1));
			output_record.setDouble("特61", FillNull(rg_extract_record.職0_4, -1));
			output_record.setDouble("特62", FillNull(rg_extract_record.職0_5, -1));
			output_record.setDouble("特63", FillNull(rg_extract_record.職0_6, -1));
			output_record.setDouble("特64", FillNull(rg_extract_record.職0_7, -1));
			output_record.setDouble("特65", FillNull(rg_extract_record.職0_8, -1));
			output_record.setDouble("特66", FillNull(rg_extract_record.職1, -1));
			output_record.setDouble("特67", FillNull(rg_extract_record.職2, -1));
			output_record.setDouble("特68", FillNull(rg_extract_record.職1_1, -1));
			output_record.setDouble("特69", FillNull(rg_extract_record.職2_1, -1));
			output_record.setDouble("特70", FillNull(rg_extract_record.職3, -1));
			output_record.setDouble("特71", FillNull(rg_extract_record.職4, -1));
			output_record.setDouble("特72", FillNull(rg_extract_record.職5, -1));
			output_record.setDouble("特73", FillNull(rg_extract_record.職6, -1));
			output_record.setDouble("特74", FillNull(rg_extract_record.職7, -1));
			output_record.setDouble("特75", CalculateQuotient(rg_extract_record.職5, 用戶期望最低薪水, -1));
			output_record.setDouble("特76", CalculateQuotient(rg_extract_record.職6, 用戶期望最低薪水, -1));
			output_record.setDouble("特77", CalculateQuotient(rg_extract_record.職7, 用戶期望最低薪水, -1));
			output_record.setDouble("特78", FillNull(rg_extract_record.職8, -1));
			output_record.setDouble("特79", FillNull(rg_extract_record.職9, -1));
			output_record.setDouble("特80", FillNull(rg_extract_record.職10, -1));
			output_record.setDouble("特81", CalculateQuotient(rg_extract_record.職8, 用戶學歷值, -1));
			output_record.setDouble("特82", CalculateQuotient(rg_extract_record.職9, 用戶學歷值, -1));
			output_record.setDouble("特83", CalculateQuotient(rg_extract_record.職10, 用戶學歷值, -1));
			output_record.setDouble("特84", FillNull(rg_extract_record.職11, -1));
			output_record.setDouble("特85", FillNull(rg_extract_record.職12, -1));
			output_record.setDouble("特86", FillNull(rg_extract_record.職13, -1));
			output_record.setDouble("特87", CalculateQuotient(rg_extract_record.職11, 用戶工作年限, -1));
			output_record.setDouble("特88", CalculateQuotient(rg_extract_record.職12, 用戶工作年限, -1));
			output_record.setDouble("特89", CalculateQuotient(rg_extract_record.職13, 用戶工作年限, -1));
			output_record.setDouble("特90", FillNull(rg_extract_record.職14, -1));
			output_record.setDouble("特91", FillNull(rg_extract_record.職15, -1));
			output_record.setDouble("特92", FillNull(rg_extract_record.職16, -1));
			output_record.setDouble("特93", CalculateQuotient(rg_extract_record.職14, 用戶年齡, -1));
			output_record.setDouble("特94", CalculateQuotient(rg_extract_record.職15, 用戶年齡, -1));
			output_record.setDouble("特95", CalculateQuotient(rg_extract_record.職16, 用戶年齡, -1));
			output_record.setDouble("特96", FillNull(rg_extract_record.職17, -1));
			output_record.setDouble("特97", FillNull(rg_extract_record.職18, -1));
			output_record.setDouble("特98", FillNull(rg_extract_record.職19, -1));
			output_record.setDouble("特99", FillNull(rg_extract_record.職20, -1));
			output_record.setDouble("特100", FillNull(rg_extract_record.職21, -1));
			output_record.setDouble("特101", FillNull(rg_extract_record.職22, -1));

			output_record.setDouble("特102", FillNull(rg_extract_record.職甲0, -1));
			output_record.setDouble("特103", FillNull(rg_extract_record.職甲1, -1));
			output_record.setDouble("特104", FillNull(rg_extract_record.職甲2, -1));
			output_record.setDouble("特105", FillNull(rg_extract_record.職甲3, -1));
			output_record.setDouble("特106", FillNull(rg_extract_record.職甲4, -1));
			output_record.setDouble("特107", CalculateQuotient(rg_extract_record.職甲2, 用戶期望最低薪水, -1));
			output_record.setDouble("特108", CalculateQuotient(rg_extract_record.職甲3, 用戶期望最低薪水, -1));
			output_record.setDouble("特109", CalculateQuotient(rg_extract_record.職甲4, 用戶期望最低薪水, -1));
			output_record.setDouble("特110", FillNull(rg_extract_record.職甲5, -1));
			output_record.setDouble("特111", FillNull(rg_extract_record.職甲6, -1));
			output_record.setDouble("特112", FillNull(rg_extract_record.職甲7, -1));
			output_record.setDouble("特113", CalculateQuotient(rg_extract_record.職甲5, 用戶學歷值, -1));
			output_record.setDouble("特114", CalculateQuotient(rg_extract_record.職甲6, 用戶學歷值, -1));
			output_record.setDouble("特115", CalculateQuotient(rg_extract_record.職甲7, 用戶學歷值, -1));
			output_record.setDouble("特116", FillNull(rg_extract_record.職甲8, -1));
			output_record.setDouble("特117", FillNull(rg_extract_record.職甲9, -1));
			output_record.setDouble("特118", FillNull(rg_extract_record.職甲10, -1));
			output_record.setDouble("特119", CalculateQuotient(rg_extract_record.職甲8, 用戶工作年限, -1));
			output_record.setDouble("特120", CalculateQuotient(rg_extract_record.職甲9, 用戶工作年限, -1));
			output_record.setDouble("特121", CalculateQuotient(rg_extract_record.職甲10, 用戶工作年限, -1));
			output_record.setDouble("特122", FillNull(rg_extract_record.職甲11, -1));
			output_record.setDouble("特123", FillNull(rg_extract_record.職甲12, -1));
			output_record.setDouble("特124", FillNull(rg_extract_record.職甲13, -1));
			output_record.setDouble("特125", FillNull(rg_extract_record.職甲14, -1));
			output_record.setDouble("特126", CalculateQuotient(rg_extract_record.職甲11, rg_extract_record.職位需求人數, -1));
			output_record.setDouble("特127", CalculateQuotient(rg_extract_record.職甲12, rg_extract_record.職位最低薪水, -1));
			output_record.setDouble("特128", CalculateQuotient(rg_extract_record.職甲13, rg_extract_record.職位最低學歷值, -1));
			output_record.setDouble("特129", CalculateQuotient(rg_extract_record.職甲14, rg_extract_record.職位工作年限, -1));
			output_record.setDouble("特130", FillNull(rg_extract_record.職甲15, -1));
			output_record.setDouble("特131", FillNull(rg_extract_record.職甲16, -1));
			output_record.setDouble("特132", FillNull(rg_extract_record.職甲17, -1));
			output_record.setDouble("特133", FillNull(rg_extract_record.職甲18, -1));
			output_record.setDouble("特134", FillNull(rg_extract_record.職甲19, -1));
			output_record.setDouble("特135", FillNull(rg_extract_record.職甲20, -1));
			output_record.setDouble("特136", FillNull(rg_extract_record.職甲256, -1));
			output_record.setDouble("特137", FillNull(rg_extract_record.職甲257, -1));

			output_record.setDouble("特138", FillNull(rg_extract_record.職乙0, -1));
			output_record.setDouble("特139", FillNull(rg_extract_record.職乙1, -1));
			output_record.setDouble("特140", FillNull(rg_extract_record.職乙2, -1));
			output_record.setDouble("特141", FillNull(rg_extract_record.職乙3, -1));
			output_record.setDouble("特142", FillNull(rg_extract_record.職乙4, -1));
			output_record.setDouble("特143", CalculateQuotient(rg_extract_record.職乙2, 用戶期望最低薪水, -1));
			output_record.setDouble("特144", CalculateQuotient(rg_extract_record.職乙3, 用戶期望最低薪水, -1));
			output_record.setDouble("特145", CalculateQuotient(rg_extract_record.職乙4, 用戶期望最低薪水, -1));
			output_record.setDouble("特146", FillNull(rg_extract_record.職乙5, -1));
			output_record.setDouble("特147", FillNull(rg_extract_record.職乙6, -1));
			output_record.setDouble("特148", FillNull(rg_extract_record.職乙7, -1));
			output_record.setDouble("特149", CalculateQuotient(rg_extract_record.職乙5, 用戶學歷值, -1));
			output_record.setDouble("特150", CalculateQuotient(rg_extract_record.職乙6, 用戶學歷值, -1));
			output_record.setDouble("特151", CalculateQuotient(rg_extract_record.職乙7, 用戶學歷值, -1));
			output_record.setDouble("特152", FillNull(rg_extract_record.職乙8, -1));
			output_record.setDouble("特153", FillNull(rg_extract_record.職乙9, -1));
			output_record.setDouble("特154", FillNull(rg_extract_record.職乙10, -1));
			output_record.setDouble("特155", CalculateQuotient(rg_extract_record.職乙8, 用戶工作年限, -1));
			output_record.setDouble("特156", CalculateQuotient(rg_extract_record.職乙9, 用戶工作年限, -1));
			output_record.setDouble("特157", CalculateQuotient(rg_extract_record.職乙10, 用戶工作年限, -1));
			output_record.setDouble("特158", FillNull(rg_extract_record.職乙11, -1));
			output_record.setDouble("特159", FillNull(rg_extract_record.職乙12, -1));
			output_record.setDouble("特160", FillNull(rg_extract_record.職乙13, -1));
			output_record.setDouble("特161", FillNull(rg_extract_record.職乙14, -1));
			output_record.setDouble("特162", CalculateQuotient(rg_extract_record.職乙11, rg_extract_record.職位需求人數, -1));
			output_record.setDouble("特163", CalculateQuotient(rg_extract_record.職乙12, rg_extract_record.職位最低薪水, -1));
			output_record.setDouble("特164", CalculateQuotient(rg_extract_record.職乙13, rg_extract_record.職位最低學歷值, -1));
			output_record.setDouble("特165", CalculateQuotient(rg_extract_record.職乙14, rg_extract_record.職位工作年限, -1));
			output_record.setDouble("特166", FillNull(rg_extract_record.職乙15, -1));
			output_record.setDouble("特167", FillNull(rg_extract_record.職乙16, -1));
			output_record.setDouble("特168", FillNull(rg_extract_record.職乙17, -1));
			output_record.setDouble("特169", FillNull(rg_extract_record.職乙18, -1));
			output_record.setDouble("特170", FillNull(rg_extract_record.職乙19, -1));
			output_record.setDouble("特171", FillNull(rg_extract_record.職乙20, -1));
			output_record.setDouble("特172", FillNull(rg_extract_record.職乙256, -1));
			output_record.setDouble("特173", FillNull(rg_extract_record.職乙257, -1));

			output_record.setDouble("特174", FillNull(rg_extract_record.職丙0, -1));
			output_record.setDouble("特175", FillNull(rg_extract_record.職丙1, -1));
			output_record.setDouble("特176", FillNull(rg_extract_record.職丙2, -1));
			output_record.setDouble("特177", FillNull(rg_extract_record.職丙3, -1));
			output_record.setDouble("特178", FillNull(rg_extract_record.職丙4, -1));
			output_record.setDouble("特179", CalculateQuotient(rg_extract_record.職丙2, 用戶期望最低薪水, -1));
			output_record.setDouble("特180", CalculateQuotient(rg_extract_record.職丙3, 用戶期望最低薪水, -1));
			output_record.setDouble("特181", CalculateQuotient(rg_extract_record.職丙4, 用戶期望最低薪水, -1));
			output_record.setDouble("特182", FillNull(rg_extract_record.職丙5, -1));
			output_record.setDouble("特183", FillNull(rg_extract_record.職丙6, -1));
			output_record.setDouble("特184", FillNull(rg_extract_record.職丙7, -1));
			output_record.setDouble("特185", CalculateQuotient(rg_extract_record.職丙5, 用戶學歷值, -1));
			output_record.setDouble("特186", CalculateQuotient(rg_extract_record.職丙6, 用戶學歷值, -1));
			output_record.setDouble("特187", CalculateQuotient(rg_extract_record.職丙7, 用戶學歷值, -1));
			output_record.setDouble("特188", FillNull(rg_extract_record.職丙8, -1));
			output_record.setDouble("特189", FillNull(rg_extract_record.職丙9, -1));
			output_record.setDouble("特190", FillNull(rg_extract_record.職丙10, -1));
			output_record.setDouble("特191", CalculateQuotient(rg_extract_record.職丙8, 用戶工作年限, -1));
			output_record.setDouble("特192", CalculateQuotient(rg_extract_record.職丙9, 用戶工作年限, -1));
			output_record.setDouble("特193", CalculateQuotient(rg_extract_record.職丙10, 用戶工作年限, -1));
			output_record.setDouble("特194", FillNull(rg_extract_record.職丙11, -1));
			output_record.setDouble("特195", FillNull(rg_extract_record.職丙12, -1));
			output_record.setDouble("特196", FillNull(rg_extract_record.職丙13, -1));
			output_record.setDouble("特197", FillNull(rg_extract_record.職丙14, -1));
			output_record.setDouble("特198", CalculateQuotient(rg_extract_record.職丙11, rg_extract_record.職位需求人數, -1));
			output_record.setDouble("特199", CalculateQuotient(rg_extract_record.職丙12, rg_extract_record.職位最低薪水, -1));
			output_record.setDouble("特200", CalculateQuotient(rg_extract_record.職丙13, rg_extract_record.職位最低學歷值, -1));
			output_record.setDouble("特201", CalculateQuotient(rg_extract_record.職丙14, rg_extract_record.職位工作年限, -1));
			output_record.setDouble("特202", FillNull(rg_extract_record.職丙15, -1));
			output_record.setDouble("特203", FillNull(rg_extract_record.職丙16, -1));
			output_record.setDouble("特204", FillNull(rg_extract_record.職丙17, -1));
			output_record.setDouble("特205", FillNull(rg_extract_record.職丙18, -1));
			output_record.setDouble("特206", FillNull(rg_extract_record.職丙19, -1));
			output_record.setDouble("特207", FillNull(rg_extract_record.職丙20, -1));
			output_record.setDouble("特208", FillNull(rg_extract_record.職丙256, -1));
			output_record.setDouble("特209", FillNull(rg_extract_record.職丙257, -1));

			output_record.setDouble("特210", FillNull(rg_extract_record.職丁0, -1));
			output_record.setDouble("特211", FillNull(rg_extract_record.職丁1, -1));
			output_record.setDouble("特212", FillNull(rg_extract_record.職丁2, -1));
			output_record.setDouble("特213", FillNull(rg_extract_record.職丁3, -1));
			output_record.setDouble("特214", FillNull(rg_extract_record.職丁4, -1));
			output_record.setDouble("特215", CalculateQuotient(rg_extract_record.職丁2, 用戶期望最低薪水, -1));
			output_record.setDouble("特216", CalculateQuotient(rg_extract_record.職丁3, 用戶期望最低薪水, -1));
			output_record.setDouble("特217", CalculateQuotient(rg_extract_record.職丁4, 用戶期望最低薪水, -1));
			output_record.setDouble("特218", FillNull(rg_extract_record.職丁5, -1));
			output_record.setDouble("特219", FillNull(rg_extract_record.職丁6, -1));
			output_record.setDouble("特220", FillNull(rg_extract_record.職丁7, -1));
			output_record.setDouble("特221", CalculateQuotient(rg_extract_record.職丁5, 用戶學歷值, -1));
			output_record.setDouble("特222", CalculateQuotient(rg_extract_record.職丁6, 用戶學歷值, -1));
			output_record.setDouble("特223", CalculateQuotient(rg_extract_record.職丁7, 用戶學歷值, -1));
			output_record.setDouble("特224", FillNull(rg_extract_record.職丁8, -1));
			output_record.setDouble("特225", FillNull(rg_extract_record.職丁9, -1));
			output_record.setDouble("特226", FillNull(rg_extract_record.職丁10, -1));
			output_record.setDouble("特227", CalculateQuotient(rg_extract_record.職丁8, 用戶工作年限, -1));
			output_record.setDouble("特228", CalculateQuotient(rg_extract_record.職丁9, 用戶工作年限, -1));
			output_record.setDouble("特229", CalculateQuotient(rg_extract_record.職丁10, 用戶工作年限, -1));
			output_record.setDouble("特230", FillNull(rg_extract_record.職丁11, -1));
			output_record.setDouble("特231", FillNull(rg_extract_record.職丁12, -1));
			output_record.setDouble("特232", FillNull(rg_extract_record.職丁13, -1));
			output_record.setDouble("特233", FillNull(rg_extract_record.職丁14, -1));
			output_record.setDouble("特234", CalculateQuotient(rg_extract_record.職丁11, rg_extract_record.職位需求人數, -1));
			output_record.setDouble("特235", CalculateQuotient(rg_extract_record.職丁12, rg_extract_record.職位最低薪水, -1));
			output_record.setDouble("特236", CalculateQuotient(rg_extract_record.職丁13, rg_extract_record.職位最低學歷值, -1));
			output_record.setDouble("特237", CalculateQuotient(rg_extract_record.職丁14, rg_extract_record.職位工作年限, -1));
			output_record.setDouble("特238", FillNull(rg_extract_record.職丁15, -1));
			output_record.setDouble("特239", FillNull(rg_extract_record.職丁16, -1));
			output_record.setDouble("特240", FillNull(rg_extract_record.職丁17, -1));
			output_record.setDouble("特241", FillNull(rg_extract_record.職丁18, -1));
			output_record.setDouble("特242", FillNull(rg_extract_record.職丁19, -1));
			output_record.setDouble("特243", FillNull(rg_extract_record.職丁20, -1));
			output_record.setDouble("特244", FillNull(rg_extract_record.職丁256, -1));
			output_record.setDouble("特245", FillNull(rg_extract_record.職丁257, -1));

			output_record.setDouble("特246", FillNull(rg_extract_record.職戊256, -1));
			output_record.setDouble("特247", FillNull(rg_extract_record.職戊257, -1));

			output_record.setDouble("特248", CalculateQuotient(rg_extract_record.職3, rg_extract_record.職4, -1));
			output_record.setDouble("特249", CalculateQuotient(rg_extract_record.職甲0, rg_extract_record.職甲1, -1));
			output_record.setDouble("特250", CalculateQuotient(rg_extract_record.職乙0, rg_extract_record.職乙1, -1));
			output_record.setDouble("特251", CalculateQuotient(rg_extract_record.職丙0, rg_extract_record.職丙1, -1));
			output_record.setDouble("特252", CalculateQuotient(rg_extract_record.職丁0, rg_extract_record.職丁1, -1));
			output_record.setDouble("特253", CalculateQuotient(rg_extract_record.職4, rg_extract_record.職位需求人數, -1));
			output_record.setDouble("特254", CalculateDifference(rg_extract_record.職9, rg_extract_record.職8, -1));
			output_record.setDouble("特255", CalculateDifference(rg_extract_record.職12, rg_extract_record.職11, -1));
			output_record.setDouble("特256", CalculateDifference(rg_extract_record.職15, rg_extract_record.職14, -1));
			output_record.setDouble("特257", CalculateDifference(rg_extract_record.職甲3, rg_extract_record.職甲2, -1));
			output_record.setDouble("特258", CalculateDifference(rg_extract_record.職甲6, rg_extract_record.職甲5, -1));
			output_record.setDouble("特259", CalculateDifference(rg_extract_record.職甲9, rg_extract_record.職甲8, -1));
			output_record.setDouble("特260", CalculateDifference(rg_extract_record.職乙3, rg_extract_record.職乙2, -1));
			output_record.setDouble("特261", CalculateDifference(rg_extract_record.職乙6, rg_extract_record.職乙5, -1));
			output_record.setDouble("特262", CalculateDifference(rg_extract_record.職乙9, rg_extract_record.職乙8, -1));
			output_record.setDouble("特263", CalculateDifference(rg_extract_record.職丙3, rg_extract_record.職丙2, -1));
			output_record.setDouble("特264", CalculateDifference(rg_extract_record.職丙6, rg_extract_record.職丙5, -1));
			output_record.setDouble("特265", CalculateDifference(rg_extract_record.職丙9, rg_extract_record.職丙8, -1));
			output_record.setDouble("特266", CalculateDifference(rg_extract_record.職丁3, rg_extract_record.職丁2, -1));
			output_record.setDouble("特267", CalculateDifference(rg_extract_record.職丁6, rg_extract_record.職丁5, -1));
			output_record.setDouble("特268", CalculateDifference(rg_extract_record.職丁9, rg_extract_record.職丁8, -1));

			context.write(output_record);
		}
	}

	private Double GetMedian(java.util.Vector<Double> vector, Double null_filler)
	{
		java.util.Vector<Double> new_vector = new java.util.Vector<Double>();
		for (Double d : vector)
		{
			if (d == null)
				continue;

			new_vector.add(d);
		}
		if (new_vector.size() == 0)
			return null_filler;

		new_vector.sort((Double a, Double b) -> Double.compare(a, b));
		return (new_vector.get((new_vector.size() - 1) / 2) + new_vector.get(new_vector.size() / 2)) / 2;
	}

	private double JudgeCity(String city, String cities)
	{
		if (city == null || cities == null)
			return -1;

		String[] split_cities_array = cities.split(",");
		if (split_cities_array.length != 3)
			return -1;

		for (int i = 0; i < 3; i++)
			if (split_cities_array[i].equals(city))
				return i;
		return 3;
	}

	private double CalculateStringMatchingScore(String a_string, String b_string)
	{
		if (a_string == null || b_string == null)
			return -1;

		java.util.HashMap<Character, Integer> a_character_hashmap = new java.util.HashMap<Character, Integer>();
		java.util.HashMap<Character, Integer> b_character_hashmap = new java.util.HashMap<Character, Integer>();
		for (char c : a_string.toCharArray())
			a_character_hashmap.put(c, 1 + a_character_hashmap.getOrDefault(c, 0));
		for (char c : b_string.toCharArray())
			b_character_hashmap.put(c, 1 + b_character_hashmap.getOrDefault(c, 0));

		double a_score = 0;
		double b_score = 0;
		for (char c : a_string.toCharArray())
			if (b_character_hashmap.containsKey(c))
				a_score++;
		for (char c : b_string.toCharArray())
			if (a_character_hashmap.containsKey(c))
				b_score++;

		return (a_score + b_score) / 2;
	}

	private double FillNull(Double a, double null_filler)
	{
		return a == null ? null_filler : a;
	}

	private double CalculateDifference(Double a, Double b, double null_filler)
	{
		if (a == null || b == null)
			return null_filler;

		return a - b;
	}

	private double CalculateQuotient(Double a, Double b, double null_filler)
	{
		if (a == null || b == null)
			return null_filler;

		if (b == 0)
			return null_filler;

		return a / b;
	}

	private class RgExtractRecord
	{
		public String 職位標識;
		public String 前職位標識;
		public String 後職位標識;
		public Long 記錄標識;
		public Long 瀏覽;
		public Long 投遞;
		public Long 認可;
		public String 職位標題;
		public String 職位城市;
		public String 職位子類;
		public Double 職位需求人數;
		public Double 職位最低薪水;
		public Double 職位最高薪水;
		public Double 職位開始日序;
		public Double 職位終止日序;
		public Double 職位出差否;
		public Double 職位工作年限;
		public String 職位關鍵詞;
		public Double 職位最低學歷值;
		public String 職位管理經驗;
		public String 職位語言需求;
		public String 職位描述;
		public Double 職0;
		public Double 職0_1;
		public Double 職0_2;
		public Double 職0_3;
		public Double 職0_4;
		public Double 職0_5;
		public Double 職0_6;
		public Double 職0_7;
		public Double 職0_8;
		public Double 職1;
		public Double 職2;
		public Double 職1_1;
		public Double 職2_1;
		public Double 職3;
		public Double 職4;
		public Double 職5;
		public Double 職6;
		public Double 職7;
		public Double 職8;
		public Double 職9;
		public Double 職10;
		public Double 職11;
		public Double 職12;
		public Double 職13;
		public Double 職14;
		public Double 職15;
		public Double 職16;
		public Double 職17;
		public Double 職18;
		public Double 職19;
		public Double 職20;
		public Double 職21;
		public Double 職22;
		public Double 職甲0;
		public Double 職甲1;
		public Double 職甲2;
		public Double 職甲3;
		public Double 職甲4;
		public Double 職甲5;
		public Double 職甲6;
		public Double 職甲7;
		public Double 職甲8;
		public Double 職甲9;
		public Double 職甲10;
		public Double 職甲11;
		public Double 職甲12;
		public Double 職甲13;
		public Double 職甲14;
		public Double 職甲15;
		public Double 職甲16;
		public Double 職甲17;
		public Double 職甲18;
		public Double 職甲19;
		public Double 職甲20;
		public Double 職甲256;
		public Double 職甲257;
		public Double 職乙0;
		public Double 職乙1;
		public Double 職乙2;
		public Double 職乙3;
		public Double 職乙4;
		public Double 職乙5;
		public Double 職乙6;
		public Double 職乙7;
		public Double 職乙8;
		public Double 職乙9;
		public Double 職乙10;
		public Double 職乙11;
		public Double 職乙12;
		public Double 職乙13;
		public Double 職乙14;
		public Double 職乙15;
		public Double 職乙16;
		public Double 職乙17;
		public Double 職乙18;
		public Double 職乙19;
		public Double 職乙20;
		public Double 職乙256;
		public Double 職乙257;
		public Double 職丙0;
		public Double 職丙1;
		public Double 職丙2;
		public Double 職丙3;
		public Double 職丙4;
		public Double 職丙5;
		public Double 職丙6;
		public Double 職丙7;
		public Double 職丙8;
		public Double 職丙9;
		public Double 職丙10;
		public Double 職丙11;
		public Double 職丙12;
		public Double 職丙13;
		public Double 職丙14;
		public Double 職丙15;
		public Double 職丙16;
		public Double 職丙17;
		public Double 職丙18;
		public Double 職丙19;
		public Double 職丙20;
		public Double 職丙256;
		public Double 職丙257;
		public Double 職丁0;
		public Double 職丁1;
		public Double 職丁2;
		public Double 職丁3;
		public Double 職丁4;
		public Double 職丁5;
		public Double 職丁6;
		public Double 職丁7;
		public Double 職丁8;
		public Double 職丁9;
		public Double 職丁10;
		public Double 職丁11;
		public Double 職丁12;
		public Double 職丁13;
		public Double 職丁14;
		public Double 職丁15;
		public Double 職丁16;
		public Double 職丁17;
		public Double 職丁18;
		public Double 職丁19;
		public Double 職丁20;
		public Double 職丁256;
		public Double 職丁257;
		public Double 職戊256;
		public Double 職戊257;
	}

	private class RankMap<Type>
	{
		public RankMap()
		{
			hash_map = new java.util.HashMap<Type, Double>();
			is_ranked = 1;
		}

		public void Put(Type key, Double value)
		{
			hash_map.put(key, value);
			is_ranked = 0;
		}

		public Double Get(Type key)
		{
			if (is_ranked == 0)
				Rank();

			return hash_map.get(key);
		}

		public int Rank()
		{
			java.util.Vector<java.util.HashMap.Entry<Type, Double>> vector = new java.util.Vector<java.util.HashMap.Entry<Type, Double>>();
			for (java.util.HashMap.Entry<Type, Double> entry : hash_map.entrySet())
				vector.add(entry);
			vector.sort((java.util.HashMap.Entry<Type, Double> a, java.util.HashMap.Entry<Type, Double> b) ->
				{
					if (a.getValue() == null && b.getValue() == null)
						return 0;
					if (a.getValue() == null)
						return 1;
					if (b.getValue() == null)
						return -1;
					return -Double.compare(a.getValue(), b.getValue());
				}
			);

			hash_map.clear();
			for (int i = 0; i < vector.size(); i++)
				hash_map.put(vector.get(i).getKey(), (double)i);

			is_ranked = 1;

			return 0;
		}

		private java.util.HashMap<Type, Double> hash_map;
		private int is_ranked;
	}
}

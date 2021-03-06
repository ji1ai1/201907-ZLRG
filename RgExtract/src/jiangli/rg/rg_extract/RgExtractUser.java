package jiangli.rg.rg_extract;

public class RgExtractUser
{
	public static void main(String[] args) throws Exception
	{
		if (args.length != 2)
		{
			System.err.println("參數錯誤！");
			System.exit(2);
		}

		com.aliyun.odps.mapred.conf.JobConf job = new com.aliyun.odps.mapred.conf.JobConf();
		job.setMapperClass(RgExtractUserMapper.class);
		job.setReducerClass(RgExtractUserReducer.class);
		job.setNumReduceTasks(1024);
		job.setMemoryForReducerJVM(4096);

		job.setMapOutputKeySchema(com.aliyun.odps.mapred.utils.SchemaUtils.fromString(
			"用戶標識:string"
			+ ",用戶城市:string"
			+ ",用戶期望城市:string"
			+ ",用戶期望行業:string"
			+ ",用戶期望職類:string"
			+ ",用戶期望最低薪水:double"
			+ ",用戶期望最高薪水:double"
			+ ",用戶當前行業:string"
			+ ",用戶當前職類:string"
			+ ",用戶當前最低薪水:double"
			+ ",用戶當前最高薪水:double"
			+ ",用戶學歷值:double"
			+ ",用戶年齡:double"
			+ ",用戶工作年限:double"
			+ ",用戶經驗:string"
		));
		job.setMapOutputValueSchema(com.aliyun.odps.mapred.utils.SchemaUtils.fromString(
			"職位標識:string"
			+ ",記錄標識:bigint"
			+ ",瀏覽:bigint"
			+ ",投遞:bigint"
			+ ",認可:bigint"
			+ ",職位標題:string"
			+ ",職位城市:string"
			+ ",職位子類:string"
			+ ",職位需求人數:double"
			+ ",職位最低薪水:double"
			+ ",職位最高薪水:double"
			+ ",職位開始日序:double"
			+ ",職位終止日序:double"
			+ ",職位出差否:double"
			+ ",職位工作年限:double"
			+ ",職位關鍵詞:string"
			+ ",職位最低學歷值:double"
			+ ",職位管理經驗:string"
			+ ",職位語言需求:string"
			+ ",職位描述:string"
			+ ",職0:double"
			+ ",職0_1:double"
			+ ",職0_2:double"
			+ ",職0_3:double"
			+ ",職0_4:double"
			+ ",職0_5:double"
			+ ",職0_6:double"
			+ ",職0_7:double"
			+ ",職0_8:double"
			+ ",職1:double"
			+ ",職2:double"
			+ ",職1_1:double"
			+ ",職2_1:double"
			+ ",職3:double"
			+ ",職4:double"
			+ ",職5:double"
			+ ",職6:double"
			+ ",職7:double"
			+ ",職8:double"
			+ ",職9:double"
			+ ",職10:double"
			+ ",職11:double"
			+ ",職12:double"
			+ ",職13:double"
			+ ",職14:double"
			+ ",職15:double"
			+ ",職16:double"
			+ ",職17:double"
			+ ",職18:double"
			+ ",職19:double"
			+ ",職20:double"
			+ ",職21:double"
			+ ",職22:double"
			+ ",職甲0:double"
			+ ",職甲1:double"
			+ ",職甲2:double"
			+ ",職甲3:double"
			+ ",職甲4:double"
			+ ",職甲5:double"
			+ ",職甲6:double"
			+ ",職甲7:double"
			+ ",職甲8:double"
			+ ",職甲9:double"
			+ ",職甲10:double"
			+ ",職甲11:double"
			+ ",職甲12:double"
			+ ",職甲13:double"
			+ ",職甲14:double"
			+ ",職甲15:double"
			+ ",職甲16:double"
			+ ",職甲17:double"
			+ ",職甲18:double"
			+ ",職甲19:double"
			+ ",職甲20:double"
			+ ",職甲256:double"
			+ ",職甲257:double"
			+ ",職乙0:double"
			+ ",職乙1:double"
			+ ",職乙2:double"
			+ ",職乙3:double"
			+ ",職乙4:double"
			+ ",職乙5:double"
			+ ",職乙6:double"
			+ ",職乙7:double"
			+ ",職乙8:double"
			+ ",職乙9:double"
			+ ",職乙10:double"
			+ ",職乙11:double"
			+ ",職乙12:double"
			+ ",職乙13:double"
			+ ",職乙14:double"
			+ ",職乙15:double"
			+ ",職乙16:double"
			+ ",職乙17:double"
			+ ",職乙18:double"
			+ ",職乙19:double"
			+ ",職乙20:double"
			+ ",職乙256:double"
			+ ",職乙257:double"
			+ ",職丙0:double"
			+ ",職丙1:double"
			+ ",職丙2:double"
			+ ",職丙3:double"
			+ ",職丙4:double"
			+ ",職丙5:double"
			+ ",職丙6:double"
			+ ",職丙7:double"
			+ ",職丙8:double"
			+ ",職丙9:double"
			+ ",職丙10:double"
			+ ",職丙11:double"
			+ ",職丙12:double"
			+ ",職丙13:double"
			+ ",職丙14:double"
			+ ",職丙15:double"
			+ ",職丙16:double"
			+ ",職丙17:double"
			+ ",職丙18:double"
			+ ",職丙19:double"
			+ ",職丙20:double"
			+ ",職丙256:double"
			+ ",職丙257:double"
			+ ",職丁0:double"
			+ ",職丁1:double"
			+ ",職丁2:double"
			+ ",職丁3:double"
			+ ",職丁4:double"
			+ ",職丁5:double"
			+ ",職丁6:double"
			+ ",職丁7:double"
			+ ",職丁8:double"
			+ ",職丁9:double"
			+ ",職丁10:double"
			+ ",職丁11:double"
			+ ",職丁12:double"
			+ ",職丁13:double"
			+ ",職丁14:double"
			+ ",職丁15:double"
			+ ",職丁16:double"
			+ ",職丁17:double"
			+ ",職丁18:double"
			+ ",職丁19:double"
			+ ",職丁20:double"
			+ ",職丁256:double"
			+ ",職丁257:double"
			+ ",職戊256:double"
			+ ",職戊257:double"
		));


		com.aliyun.odps.mapred.utils.InputUtils.addTable(com.aliyun.odps.data.TableInfo.builder().tableName(args[0]).build(), job);
		com.aliyun.odps.mapred.utils.OutputUtils.addTable(com.aliyun.odps.data.TableInfo.builder().tableName(args[1]).build(), job);
		com.aliyun.odps.mapred.JobClient.runJob(job);
	}
}

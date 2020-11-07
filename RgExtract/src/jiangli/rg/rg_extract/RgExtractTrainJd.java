package jiangli.rg.rg_extract;

public class RgExtractTrainJd
{
	public static void main(String[] args) throws Exception
	{
		if (args.length != 3)
		{
			System.err.println("參數錯誤！");
			System.exit(2);
		}

		com.aliyun.odps.mapred.conf.JobConf job = new com.aliyun.odps.mapred.conf.JobConf();
		job.setMapperClass(RgExtractTrainJdMapper.class);
		job.setReducerClass(RgExtractTrainJdReducer.class);
		job.setNumReduceTasks(Integer.parseInt(args[2]));
		job.setMemoryForReducerJVM(4096);

		job.setMapOutputKeySchema(com.aliyun.odps.mapred.utils.SchemaUtils.fromString("欄0:bigint"));
		job.setMapOutputValueSchema(com.aliyun.odps.mapred.utils.SchemaUtils.fromString(
			"職位標識:string"
			+ ",職位標題:string"
			+ ",職位子類:string"
			+ ",職位城市:string"
			+ ",職位描述:string"
			+ ",用戶標識:string"
			+ ",標特標誌:bigint"
			+ ",瀏覽:bigint"
			+ ",投遞:bigint"
			+ ",認可:bigint"
		));

		com.aliyun.odps.data.TableInfo table_info = com.aliyun.odps.data.TableInfo.builder().tableName(args[0]).build();
		table_info.setLable(args[2]);
		com.aliyun.odps.mapred.utils.InputUtils.addTable(table_info, job);
		com.aliyun.odps.mapred.utils.OutputUtils.addTable(com.aliyun.odps.data.TableInfo.builder().tableName(args[1]).build(), job);
		com.aliyun.odps.mapred.JobClient.runJob(job);
	}
}

package jiangli.rg.rg_import;

public class RgImport
{
	public static void main(String[] args) throws Exception
	{
		if (args.length != 2)
		{
			System.err.println("參數錯誤！");
			System.exit(2);
		}

		com.aliyun.odps.mapred.conf.JobConf job = new com.aliyun.odps.mapred.conf.JobConf();
		job.setMapperClass(RgImportMapper.class);
		job.setReducerClass(RgImportReducer.class);
		job.setNumReduceTasks(1);
		job.setMemoryForReducerJVM(4096);

		job.setMapOutputKeySchema(com.aliyun.odps.mapred.utils.SchemaUtils.fromString("零:bigint"));
		job.setMapOutputValueSchema(com.aliyun.odps.mapred.utils.SchemaUtils.fromString(
			"職位標識:string"
			+ ",用戶標識:string"
			+ ",瀏覽:bigint"
			+ ",投遞:bigint"
			+ ",認可:bigint"
		));

		com.aliyun.odps.mapred.utils.InputUtils.addTable(com.aliyun.odps.data.TableInfo.builder().tableName(args[0]).build(), job);
		com.aliyun.odps.mapred.utils.OutputUtils.addTable(com.aliyun.odps.data.TableInfo.builder().tableName(args[1]).build(), job);
		com.aliyun.odps.mapred.JobClient.runJob(job);
	}
}

package jiangli.rg.rg_preprocess;

class RgPreprocessJd
{
	public static void main(String[] args) throws Exception
	{
		if (args.length != 2)
		{
			System.err.println("參數錯誤！");
			System.exit(2);
		}

		com.aliyun.odps.mapred.conf.JobConf job = new com.aliyun.odps.mapred.conf.JobConf();
		job.setMapperClass(RgPreprocessJdMapper.class);
		job.setNumReduceTasks(0);

		com.aliyun.odps.mapred.utils.InputUtils.addTable(com.aliyun.odps.data.TableInfo.builder().tableName(args[0]).build(), job);
		com.aliyun.odps.mapred.utils.OutputUtils.addTable(com.aliyun.odps.data.TableInfo.builder().tableName(args[1]).build(), job);
		com.aliyun.odps.mapred.JobClient.runJob(job);
	}
}

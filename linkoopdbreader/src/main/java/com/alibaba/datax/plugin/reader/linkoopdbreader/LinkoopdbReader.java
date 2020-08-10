package com.alibaba.datax.plugin.reader.linkoopdbreader;

import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.reader.CommonRdbmsReader;
import com.alibaba.datax.plugin.rdbms.reader.Constant;
import com.alibaba.datax.plugin.rdbms.reader.Key;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;

public class  LinkoopdbReader extends Reader {
	private static final DataBaseType DATABASE_TYPE = DataBaseType.Linkoopdb;
	public static class Job extends Reader.Job {
		private static final Logger LOG = LoggerFactory
				.getLogger(Job.class);

		private Configuration originalConfig = null;
		private CommonRdbmsReader.Job commonRdbmsReaderJob;

		@Override
		public void init() {

			this.originalConfig = super.getPluginJobConf();
			this.originalConfig.set(Constant.FETCH_SIZE, 5);
			this.commonRdbmsReaderJob = new CommonRdbmsReader.Job(DATABASE_TYPE);
			this.commonRdbmsReaderJob.init(this.originalConfig);
		}

		@Override
		public void prepare() {
			init();
			this.commonRdbmsReaderJob.preCheck(this.originalConfig,DATABASE_TYPE);
			saveDdl(this.originalConfig);
		}

		private void saveDdl(Configuration originalConfig) {
			String driver = DATABASE_TYPE.getDriverClassName();
			String jdbcurl = originalConfig.getString("connection[0].jdbcUrl");
			String username = originalConfig.getString(Key.USERNAME);
			String password = originalConfig.getString(Key.PASSWORD);
			String table = originalConfig.getString("connection[0].table[0]");
			String filePath = originalConfig.getString(com.alibaba.datax.plugin.reader.linkoopdbreader.Key.PATH);
			String ddlFile = filePath + "/" + "ddl_" + table;
			System.out.println(table + jdbcurl + ddlFile + driver + username + password);
			System.out.println(originalConfig.beautify());
			LinkoopDBUtils.saveDdlInfo(ddlFile, table, driver, jdbcurl, username, password);
		}

		@Override
		public List<Configuration> split(int adviceNumber) {
			return this.commonRdbmsReaderJob.split(this.originalConfig, adviceNumber);
		}

		@Override
		public void post() {
			this.commonRdbmsReaderJob.post(this.originalConfig);
		}

		@Override
		public void destroy() {
			this.commonRdbmsReaderJob.destroy(this.originalConfig);
		}

	}

	public static class Task extends Reader.Task {
		private Configuration readerSliceConfig;
		private CommonRdbmsReader.Task commonRdbmsReaderTask;

		@Override
		public void init() {
			this.readerSliceConfig = super.getPluginJobConf();
			this.commonRdbmsReaderTask = new CommonRdbmsReader.Task(DATABASE_TYPE,super.getTaskGroupId(), super.getTaskId());
			this.commonRdbmsReaderTask.init(this.readerSliceConfig);
		}

		@Override
		public void prepare() {
		}

		@Override
		public void startRead(RecordSender recordSender) {
			int fetchSize = 5;
			this.commonRdbmsReaderTask.startRead(this.readerSliceConfig, recordSender,
					super.getTaskPluginCollector(), fetchSize);
		}

		@Override
		public void post() {
			this.commonRdbmsReaderTask.post(this.readerSliceConfig);
		}

		@Override
		public void destroy() {
			this.commonRdbmsReaderTask.destroy(this.readerSliceConfig);
		}
	}
}

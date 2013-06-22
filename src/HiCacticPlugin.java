import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.log4j.Logger;

// -n plt.pubigtjnl1
public class HiCacticPlugin {
	static Logger log = Logger.getLogger(HiCacticPlugin.class);
	private String msgInf = null;

	public static void main(String[] args) {
		if (args.length < 1) {
			log.error("args.length < 1");
			printHelp();
			System.exit(8);
		}

		HiCacticPlugin plugin = new HiCacticPlugin();

		try {
			int ret = plugin.process(args);
			if (ret != 0) {
				log.error("ret:[" + ret + "], msgInf:[" + plugin.msgInf + "]");
				printHelp();
				System.out.println("### " + plugin.msgInf);
			} else {
				log.info("ret:[" + ret + "], msgInf:[" + plugin.msgInf + "]");
				System.out.println(plugin.msgInf);
			}
			System.exit(ret);
		} catch (Throwable e) {
			log.error(e, e);
			System.exit(8);
		}
	}

	public int process(String[] args) throws Exception {
		int critical = -1;
		int warning = -1;
		String fldNam = null;
		String sqlName = null;
		ArrayList<HiParam> params = new ArrayList<HiParam>();
		for (int i = 0; i < args.length; i++) {
			if ("-h".equals(args[i])) {
				printHelp();
				return 4;
			}

			if ("-n".equals(args[i])) {
				if (i + 1 >= args.length) {
					msgInf = "-n value not define";
					return 8;
				}
				sqlName = args[i + 1];
				i++;
			}
		}

		if (sqlName == null) {
			msgInf = "-n not define";
			return 8;
		}

		HiSqlConfig sqlConfig = new HiSqlConfig();
		sqlConfig.load("sqlconfig.xml");
		SqlConfigItem sqlConfigItem = sqlConfig.getSql(sqlName);
		if (sqlConfigItem == null) {
			msgInf = "sql name:[" + sqlName + "] not exists in sqlconfig.xml";
			return 8;
		}
		SqlConfigDataSource dataSource = sqlConfig
				.getDataSource(sqlConfigItem.group.dsName);
		if (dataSource == null) {
			msgInf = "dataSource name:[" + sqlConfigItem.group.dsName
					+ "] not exists in sqlconfig.xml";
			return 8;
		}
		StringBuffer perfInfoStr = new StringBuffer();
		List<LinkedHashMap> recs = query(sqlConfigItem.sql, dataSource);
		for (LinkedHashMap map : recs) {
			if (recs.size() == 1) {
				// 
				Iterator iter = map.keySet().iterator();
				while (iter.hasNext()) {
					String key = (String) iter.next();
					perfInfoStr.append(key + ":" + (String) map.get(key) + " ");
				}
				break;
			} else {
				// 
				Iterator iter = map.keySet().iterator();
				if (map.size() >= 2) {
					String name1 = (String) iter.next();
					String value1 = (String) map.get(name1);
					String name2 = (String) iter.next();
					String value2 = (String) map.get(name2);
					perfInfoStr.append(name1 + "_" + value1 + ":" + value2 + " ");
				}
			}
		}
		
		msgInf = perfInfoStr.toString();
		return 0;
	}

	public List<LinkedHashMap> query(String sql, SqlConfigDataSource dataSource)
			throws ClassNotFoundException, SQLException {
		Class.forName(dataSource.driverClass);
		Connection conn = DriverManager.getConnection(dataSource.jdbcUrl,
				dataSource.user, dataSource.password);
		log.info("sql:[" + sql + "]");

		PreparedStatement pst = conn.prepareStatement(sql);
		ResultSet rs = pst.executeQuery();
		ResultSetMetaData rsmd = rs.getMetaData();
		List<LinkedHashMap> list = new ArrayList<LinkedHashMap>();
		while (rs.next()) {
			LinkedHashMap<String, String> rec = new LinkedHashMap<String, String>();
			for (int i = 1; i <= rsmd.getColumnCount(); i++) {
				String columnName = rsmd.getColumnName(i);
				String value = rs.getString(i);
				rec.put(columnName, value);
			}
			list.add(rec);
		}
		conn.close();
		return list;
	}

	public static void printHelp() {
		System.out.println("### cactic_jsql v0.5 (ics-cactic-plugins 0.5)");
		System.out.println("### Copyright (c) 1999-2013 HISUN ICS ");
		System.out.println("### <ics-devel@hisuntech.com>");
		System.out.println("### This plugin execute sql by jdbc");
		System.out.println("###");
		System.out.println("###");
		System.out.println("### Usage:");
		System.out
				.println("### cactic_jsql [-h] -n sql name");
		System.out.println("###");
	}

}

 

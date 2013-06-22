import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.log4j.Logger;

// -w 20 -c 30 -f sts.sts_1 -C  -w 20 -c 30 -f sts.sts_2   -n pubigtjnl
public class HiNagiosPlugin {
	static Logger log = Logger.getLogger(HiNagiosPlugin.class);
	private String msgInf = null;

	public static void main(String[] args) {
		if (args.length < 3) {
			log.error("args.length < 3");
			printHelp();
			System.exit(8);
		}

		HiNagiosPlugin plugin = new HiNagiosPlugin();

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

			if ("-c".equals(args[i])) {
				if (i + 1 >= args.length) {
					msgInf = "-c value not define";
					return 8;
				}
				critical = NumberUtils.toInt(args[i + 1]);
				i++;
			}

			if ("-w".equals(args[i])) {
				if (i + 1 >= args.length) {
					msgInf = "-w value not define";
					return 8;
				}
				warning = NumberUtils.toInt(args[i + 1]);
				i++;
			}

			if ("-f".equals(args[i])) {
				if (i + 1 >= args.length) {
					msgInf = "-f value not define";
					return 8;
				}
				fldNam = args[i + 1];
				i++;
			}

			if ("-n".equals(args[i])) {
				if (i + 1 >= args.length) {
					msgInf = "-n value not define";
					return 8;
				}
				sqlName = args[i + 1];
				i++;
			}

			if ("-C".equals(args[i])) {
				if (warning == -1 || critical == -1) {
					msgInf = "-c | -w  not define";
					return 8;
				}
				// clear 
				HiParam param = new HiParam(warning, critical, fldNam);
				params.add(param);
				warning = -1;
				critical = -1;
				fldNam = null;
			}
		}

		if (warning == -1 || critical == -1 || fldNam == null) {
			msgInf = "-c | -w not define";
			return 8;
		}

		HiParam param = new HiParam(warning, critical, fldNam);
		params.add(param);

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
		String minValue = "";
		String maxValue = "";
		SqlConfigDataSource dataSource = sqlConfig
				.getDataSource(sqlConfigItem.group.dsName);
		if (dataSource == null) {
			msgInf = "dataSource name:[" + sqlConfigItem.group.dsName
					+ "] not exists in sqlconfig.xml";
			return 8;
		}

		List recs = query(sqlConfigItem.sql, dataSource);

		StringBuffer perfInfo = new StringBuffer();
		StringBuffer statusInfo = new StringBuffer();
		int ret = 0;

		for (HiParam param1 : params) {
			String tmp = getValue(recs, param1.fldNam);
			if (tmp == null) {
				continue;
			}
			int value = NumberUtils.toInt(tmp);
			if (value >= warning && value < critical) {
				if (ret < 1) {
					ret = 1;
				}
				statusInfo.append("WARNING - " + sqlConfigItem.group.desc + "."
						+ sqlConfigItem.desc + ": ");
			} else if (value >= critical) {
				ret = 2;
				statusInfo.append("CRITICAL - " + sqlConfigItem.group.desc
						+ "." + sqlConfigItem.desc + ": ");
			} else {
				statusInfo.append("OK - " + sqlConfigItem.group.desc + "."
						+ sqlConfigItem.desc + ": ");
			}

			// 
			String desc = param1.fldNam;
			SqlConfigMappingItem mappingItem = sqlConfig.getMappingItemByNam(sqlName, param1.fldNam);
			if( mappingItem != null ) {
				desc = mappingItem.value;
			}
			statusInfo.append(desc + " " + value + "\n");
			
			perfInfo.append(param1.fldNam + "=" + value + ";" + warning + ";"
					+ critical + ";" + minValue + ";" + maxValue + "\n");
		}
		String statusInfoStr = StringUtils.chop(statusInfo.toString());
		String perfInfoStr = StringUtils.chop(perfInfo.toString());
		msgInf = statusInfoStr + " | " + perfInfoStr;
		return ret;
	}

	public String getValue(List<HashMap> recs, String name) {
		log.info("getValue:[" + recs + "], name:[" + name + "]");
		int idx = name.indexOf(".");
		String fldNam = name;
		String fldVal = null;
		if (idx != -1) {
			fldNam = name.substring(0, idx);
			fldVal = name.substring(idx + 1);
			log.info("fldNam:[" + fldNam + "], fldVal:[" + fldVal + "]");
		}
		for (HashMap rec : recs) {
			String value = (String) rec.get(fldNam.toUpperCase());
			if (fldVal == null) {
				return value;
			}

			if (!StringUtils.equalsIgnoreCase(value, fldVal)) {
				continue;
			}
			// 获取下一个
			Iterator iter = rec.keySet().iterator();
			while (iter.hasNext()) {
				name = (String) iter.next();
				if (name.equalsIgnoreCase(fldNam)) {
					continue;
				}
				return (String) rec.get(name);
			}
		}
		return null;
	}

	public List query(String sql, SqlConfigDataSource dataSource)
			throws ClassNotFoundException, SQLException {
		Class.forName(dataSource.driverClass);
		Connection conn = DriverManager.getConnection(dataSource.jdbcUrl,
				dataSource.user, dataSource.password);
		log.info("sql:[" + sql + "]");

		PreparedStatement pst = conn.prepareStatement(sql);
		ResultSet rs = pst.executeQuery();
		ResultSetMetaData rsmd = rs.getMetaData();
		List<HashMap> list = new ArrayList<HashMap>();
		while (rs.next()) {
			HashMap<String, String> rec = new HashMap<String, String>();
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
		System.out.println("### nagios_jsql v0.5 (ics-nagios-plugins 0.5)");
		System.out.println("### Copyright (c) 1999-2013 HISUN ICS ");
		System.out.println("### <ics-devel@hisuntech.com>");
		System.out.println("### This plugin execute sql by jdbc");
		System.out.println("###");
		System.out.println("###");
		System.out.println("### Usage:");
		System.out
				.println("### nagios_jsql -w limit -c limit [-f fldNam] [-C  -w limit -c limit [-f fldNam] [...]] -n sql name");
		System.out.println("###");
	}

}

class HiParam {
	int warning;
	int critical;
	String fldNam;

	public HiParam(int warning, int critical, String fldNam) {
		this.warning = warning;
		this.critical = critical;
		this.fldNam = fldNam;
	}
}

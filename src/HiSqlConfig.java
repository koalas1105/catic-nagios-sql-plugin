import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

public class HiSqlConfig {
	private HashMap<String, SqlConfigGroup> sqls = new HashMap<String, SqlConfigGroup>();
	private HashMap<String, SqlConfigDataSource> dataSources = new HashMap<String, SqlConfigDataSource>();
	private HashMap<String, SqlConfigMapping> mappings = new HashMap<String, SqlConfigMapping>();

	static Logger log = Logger.getLogger(HiNagiosPlugin.class);

	public void load(String fileName) throws Exception {
		SAXReader reader = new SAXReader();
		Document doc = reader.read(new File(fileName));
		parseDataSource(doc.getRootElement());
		parseSql(doc.getRootElement());
		parseMapping(doc.getRootElement());
	}

	private void parseDataSource(Element root) throws Exception {
		Element dataSourcesElement = root.element("DataSources");

		Iterator iter = dataSourcesElement.elementIterator("DataSource");
		while (iter.hasNext()) {
			Element dataSourceElement = (Element) iter.next();
			String name = dataSourceElement.attributeValue("name");
			String driverClass = dataSourceElement.elementText("DriverClass");
			String jdbcUrl = dataSourceElement.elementText("JdbcUrl");
			String user = dataSourceElement.elementText("User");
			String password = dataSourceElement.elementText("Password");
			if (StringUtils.isBlank(name)) {
				throw new Exception(
						"### ERROR DataSource doesn't contain attribute name");
			}

			if (StringUtils.isBlank(driverClass)
					|| StringUtils.isBlank(jdbcUrl)
					|| StringUtils.isBlank(user)
					|| StringUtils.isBlank(password)) {
				throw new Exception(
						"### ERROR DataSource doesn't contain child element DriverClass|JdbcUrl|User|Password");
			}

			dataSources.put(name, new SqlConfigDataSource(driverClass, jdbcUrl,
					user, password));
		}
	}

	private void parseSql(Element root) throws Exception {
		Element sqlElement = root.element("Sql");
		Iterator groupIter = sqlElement.elementIterator("Group");
		while (groupIter.hasNext()) {
			Element groupElement = (Element) groupIter.next();
			String name = groupElement.attributeValue("name");
			String dsName = groupElement.attributeValue("dsName");
			String desc = groupElement.attributeValue("desc");
			if (StringUtils.isBlank(name) || StringUtils.isBlank(dsName)) {
				throw new Exception("Group attribute name|dsName is empty");
			}

			if (desc == null) {
				desc = name;
			}
			SqlConfigGroup configGroup = new SqlConfigGroup(name, dsName, desc);

			Iterator itemIter = groupElement.elementIterator("Item");
			while (itemIter.hasNext()) {
				Element itemElement = (Element) itemIter.next();
				name = itemElement.attributeValue("name");
				String sql = itemElement.attributeValue("sql");
				desc = itemElement.attributeValue("desc");
				if (StringUtils.isBlank(name) || StringUtils.isBlank(sql)) {
					throw new Exception("Item attribute name|sql is empty");
				}

				if (desc == null) {
					desc = name;
				}

				configGroup.add(new SqlConfigItem(name, sql, desc));

				sqls.put(configGroup.name, configGroup);
			}

		}
	}

	private void parseMapping(Element root) throws Exception {
		Element sqlElement = root.element("Mappings");
		Iterator mappingIter = sqlElement.elementIterator("Mapping");

		while (mappingIter.hasNext()) {
			Element mappingElement = (Element) mappingIter.next();
			String name = mappingElement.attributeValue("name");
			if (StringUtils.isBlank(name)) {
				throw new Exception("Mapping attribute name|value is empty");

			}
			SqlConfigMapping mapping = new SqlConfigMapping(name);
			mappings.put(name, mapping);
			Iterator itemIter = mappingElement.elementIterator("Item");
			while (itemIter.hasNext()) {
				Element itemElement = (Element) itemIter.next();
				name = itemElement.attributeValue("name");
				String value = itemElement.attributeValue("value");
				if (StringUtils.isBlank(name) || StringUtils.isBlank(value)) {
					throw new Exception("Item attribute name|value is empty");

				}

				String desc = itemElement.attributeValue("desc");
				if (StringUtils.isBlank(desc)) {
					desc = name;
				}
				mapping.add(new SqlConfigMappingItem(name, value, desc));
			}

		}
	}

	public SqlConfigMapping getMapping(String name) throws Exception {
		return mappings.get(name);
	}

	public SqlConfigMappingItem getMappingItemByNam(String mappingName,
			String itemName) throws Exception {
		log.info("getMappingItemByNam, mappingName:[" + mappingName + "], itemName:[" + itemName + "]");
		SqlConfigMapping mapping = getMapping(mappingName);
		if (mapping == null) {
			return null;
		}
		return mapping.getByNam(itemName);
	}

	public SqlConfigMappingItem getMappingItemByVal(String mappingName,
			String itemValue) throws Exception {
		SqlConfigMapping mapping = getMapping(mappingName);
		if (mapping == null) {
			return null;
		}
		return mapping.getByVal(itemValue);
	}

	public SqlConfigItem getSql(String name) throws Exception {
		int idx = name.indexOf(".");
		if (idx == -1) {
			throw new Exception("name doesn't contain . character");
		}
		String groupName = name.substring(0, idx);
		String itemName = name.substring(idx + 1);
		SqlConfigGroup configGroup = sqls.get(groupName);
		if (configGroup == null) {
			throw new Exception("groupName:[" + groupName + "] not found");
		}
		return configGroup.get(itemName);
	}

	public SqlConfigDataSource getDataSource(String name) {
		return dataSources.get(name);
	}

}

class SqlConfigItem {
	String name;
	String sql;
	String desc;
	SqlConfigGroup group;

	public SqlConfigItem(String name, String sql, String desc) {
		this.name = name;
		this.sql = sql;
		this.desc = desc;
	}

	public void setGroup(SqlConfigGroup group) {
		this.group = group;
	}
}

class SqlConfigGroup {
	String name;
	String dsName;
	String desc;
	HashMap<String, SqlConfigItem> items = new HashMap<String, SqlConfigItem>();

	public SqlConfigGroup(String name, String dsName, String desc) {
		this.name = name;
		this.dsName = dsName;
		this.desc = desc;
	}

	public void add(SqlConfigItem item) {
		item.setGroup(this);
		items.put(item.name, item);
	}

	public SqlConfigItem get(String name) {
		return items.get(name);
	}
}

class SqlConfigDataSource {
	String driverClass;
	String jdbcUrl;
	String user;
	String password;

	public SqlConfigDataSource(String driverClass, String jdbcUrl, String user,
			String password) {
		this.driverClass = driverClass;
		this.jdbcUrl = jdbcUrl;
		this.user = user;
		this.password = password;
	}
}

class SqlConfigMapping {
	String name;
	ArrayList<SqlConfigMappingItem> items = new ArrayList<SqlConfigMappingItem>();

	public SqlConfigMapping(String name) {
		this.name = name;
	}

	public void add(SqlConfigMappingItem item) {
		items.add(item);
	}

	public SqlConfigMappingItem getByNam(String name) {
		for (SqlConfigMappingItem item : items) {
			if (item.name.equalsIgnoreCase(name)) {
				return item;
			}
		}
		return null;
	}

	public SqlConfigMappingItem getByVal(String value) {
		for (SqlConfigMappingItem item : items) {
			if (item.value.equalsIgnoreCase(value)) {
				return item;
			}
		}
		return null;
	}

}

class SqlConfigMappingItem {
	String name;
	String value;
	String desc;

	public SqlConfigMappingItem(String name, String value, String desc) {
		this.name = name;
		this.value = value;
		this.desc = desc;
	}

}

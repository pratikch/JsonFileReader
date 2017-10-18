
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Parser;
import org.trafodion.sql.udr.TypeInfo;
import org.trafodion.sql.udr.UDRException;

public class TypeDecoder {

	private static final String MAPPING_KEY_COLUMN_SEPARATOR = "=";
	private String schemaLocation;
	private String mappingLocation;

	private Map<String, TypeInfo> columnNameDatatypeMap = null;
	private Map<String, String> fieldColumnNameMap = null;
	private Map<String, Schema.Type> fieldJsonTypeMap = null;

	public TypeDecoder(String schema, String mapping) throws IOException, UDRException {
		this.schemaLocation = schema;
		this.mappingLocation = mapping;
		this.parse();
	}

	public Map<String, TypeInfo> getColumnNameDatatypeMap() {
		return columnNameDatatypeMap;
	}

	public Map<String, String> getFieldColumnNameMap() {
		return fieldColumnNameMap;
	}

	public Map<String, Schema.Type> getFieldJsonTypeMap() {
		return fieldJsonTypeMap;
	}

	private Map<String, TypeInfo> parse() throws IOException, UDRException {
		Map<String, TypeInfo> columnNameTypeMap = null;
		Schema.Type type = null;
		Map<String, Schema.Type> fieldTypeMap = null;
		if (fieldJsonTypeMap == null) {
			Schema schema = new Parser().parse(new File(schemaLocation));
			List<Field> fields = schema.getFields();
			fieldTypeMap = new HashMap<String, Schema.Type>();
			for (Field field : fields) {
				type = field.schema().getType();
				fieldTypeMap.put(field.name(), type);
			}
			fieldJsonTypeMap = fieldTypeMap;
		} else {
			fieldTypeMap = fieldJsonTypeMap;
		}

		Map<String, String> fieldColumnMap = null;
		if (fieldColumnNameMap == null) {
			fieldColumnMap = new LinkedHashMap<String, String>();
			BufferedReader br = new BufferedReader(new FileReader(mappingLocation));
			String sCurrentLine;
			String[] tempArray = null;
			while ((sCurrentLine = br.readLine()) != null) {
				tempArray = sCurrentLine.trim().split(MAPPING_KEY_COLUMN_SEPARATOR);
				if (tempArray.length > 0 && tempArray.length == 2) {
					fieldColumnMap.put(tempArray[0], tempArray[1]);
				}
			}
			fieldColumnNameMap = fieldColumnMap;
		} else {
			fieldColumnMap = fieldColumnNameMap;
		}

		if (columnNameDatatypeMap == null) {
			columnNameTypeMap = new LinkedHashMap<String, TypeInfo>();
			TypeInfo.SQLTypeCode typeCode = TypeInfo.SQLTypeCode.UNDEFINED_SQL_TYPE;
			int length = 0;
			boolean nullable = true;
			int scale = 0;
			TypeInfo.SQLCharsetCode charset = TypeInfo.SQLCharsetCode.CHARSET_UTF8;
			TypeInfo.SQLIntervalCode intervalCode = TypeInfo.SQLIntervalCode.UNDEFINED_INTERVAL_CODE;
			int precision = 0;
			TypeInfo tempTypeInfo = null;
			for (Map.Entry<String, String> fieldColumnEntry : fieldColumnMap.entrySet()) {

				typeCode = TypeInfo.SQLTypeCode.UNDEFINED_SQL_TYPE;
				length = 0;
				nullable = true;
				scale = 0;
				charset = TypeInfo.SQLCharsetCode.CHARSET_UTF8;
				intervalCode = TypeInfo.SQLIntervalCode.UNDEFINED_INTERVAL_CODE;
				precision = 0;

				type = fieldTypeMap.get(fieldColumnEntry.getKey());
				switch (type) {
				case ARRAY:
				case BOOLEAN:
				case ENUM:
				case FIXED:
				case MAP:
				case RECORD:
				case UNION:
				case BYTES:
				case NULL:
					throw new UDRException(38002, "Not supported dataType");
				case DOUBLE:
					typeCode = TypeInfo.SQLTypeCode.DOUBLE_PRECISION;
					length = 10;
					precision = 2;
					tempTypeInfo = new TypeInfo(typeCode, length, nullable, scale, charset, intervalCode, precision);
					break;
				case FLOAT:
					typeCode = TypeInfo.SQLTypeCode.DOUBLE_PRECISION;
					length = 10;
					precision = 2;
					tempTypeInfo = new TypeInfo(typeCode, length, nullable, scale, charset, intervalCode, precision);
					break;
				case INT:
					typeCode = TypeInfo.SQLTypeCode.INT;
					length = 10;
					tempTypeInfo = new TypeInfo(typeCode, length, nullable, scale, charset, intervalCode, precision);
					break;
				case LONG:
					typeCode = TypeInfo.SQLTypeCode.LARGEINT;
					length = 10;
					tempTypeInfo = new TypeInfo(typeCode, length, nullable, scale, charset, intervalCode, precision);
					break;
				case STRING:
					typeCode = TypeInfo.SQLTypeCode.VARCHAR;
					length = 1024;
					tempTypeInfo = new TypeInfo(typeCode, length, nullable, scale, charset, intervalCode, precision);
					break;
				default:
					throw new UDRException(38001, "Not supported dataType in default case");
				}// switch

				columnNameTypeMap.put(fieldColumnEntry.getValue(), tempTypeInfo);
			}
			columnNameDatatypeMap = columnNameTypeMap;
		} else {
			columnNameTypeMap = columnNameDatatypeMap;
		}

		// Following data types still needs to be worked on
		// case 'N':
		// typeCode = TypeInfo.SQLTypeCode.NUMERIC;
		// precision = decodeNumber();
		// if (pos_ < len_ && colEncodings_.charAt(pos_) == '.') {
		// pos_++;
		// scale = decodeNumber();
		// }
		// break;
		// case 'S':
		// typeCode = TypeInfo.SQLTypeCode.TIMESTAMP;
		// scale = decodeNumber();
		// break;
		// case 'T':
		// typeCode = TypeInfo.SQLTypeCode.TIME;
		// break;
		// default:
		// throw new UDRException(38001,
		// "Expecting 'C', 'D', 'F', 'I', 'L', 'N', 'S', or 'T' in fourth argument, got
		// %s",
		// colEncodings_.substring(pos_ - 1));
		// }

		return columnNameTypeMap;
	}
}

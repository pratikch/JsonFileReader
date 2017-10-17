/**
 * 
 */


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.trafodion.sql.udr.ColumnInfo;
import org.trafodion.sql.udr.TypeInfo;
import org.trafodion.sql.udr.UDR;
import org.trafodion.sql.udr.UDRException;
import org.trafodion.sql.udr.UDRInvocationInfo;
import org.trafodion.sql.udr.UDRPlanInfo;

/**
 * @author vagrant
 *
 */
public class JsonFileReader extends UDR {

	@Override
	public void describeParamsAndColumns(UDRInvocationInfo info) throws UDRException {
		InputParameters input = InputParameters.parseInputParameters(info);
		try {
			TypeDecoder decoder = new TypeDecoder(input.getSchemaLocation(), input.getFieldColumnMappingLocation());
			Map<String, TypeInfo> columnMap = decoder.getColumnNameDatatypeMap();
			for (Map.Entry<String, TypeInfo> entry : columnMap.entrySet()) {
				info.out().addColumn(new ColumnInfo(entry.getKey(), entry.getValue()));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private Properties getConnectionProperties(InputParameters input) {
		Properties props = new Properties();
		props.put("bootstrap.servers", input.getConnectionString());
		props.put("group.id", "EsgynUDF_Group_" + input.getGroupId());
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		return props;

	}

	@Override
	public void processData(UDRInvocationInfo info, UDRPlanInfo plan) throws UDRException {
		InputParameters input = InputParameters.parseInputParameters(info);

		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(getConnectionProperties(input));
		try {
			ObjectMapper mapper = new ObjectMapper();
			ConsumerRecords<String, String> records = null;
			JsonNode rootNode = null;
			TypeDecoder decoder = new TypeDecoder(input.getSchemaLocation(), input.getFieldColumnMappingLocation());
			Map<String, String> fieldNameColumnMap = decoder.getFieldColumnNameMap();
			Map<String, Schema.Type> fieldJsonTypeMap = decoder.getFieldJsonTypeMap();
			String fieldName = null;
			StringBuilder sb = new StringBuilder(512);
			JsonNode tempNode = null;
			while (true) {
				records = consumer.poll(100);

				for (ConsumerRecord<String, String> record : records) {
					System.out.println(record.value());
					rootNode = mapper.readTree(record.value());
					sb = new StringBuilder(512);
					for (Map.Entry<String, String> fieldColumnEntry : fieldNameColumnMap.entrySet()) {
						fieldName = fieldColumnEntry.getKey();
						tempNode = rootNode.get(fieldName);

						if (tempNode != null) {
							switch (fieldJsonTypeMap.get(fieldName)) {
							case ARRAY:
							case BOOLEAN:
							case BYTES:
							case ENUM:
							case FIXED:
							case MAP:
							case NULL:
							case RECORD:
							case UNION:
								throw new Exception("Not supported type");
							case DOUBLE:
							case FLOAT:
								sb.append(tempNode.getDoubleValue()).append("|");
								break;
							case INT:
								sb.append(tempNode.getIntValue()).append("|");
								break;
							case LONG:
								sb.append(tempNode.getLongValue()).append("|");
								break;
							case STRING:
								sb.append(tempNode.getTextValue()).append("|");
								break;
							default:
								throw new Exception("Not supported type");
							}
						} else {
							sb.append("|");
						}
					}
					info.out().setFromDelimitedRow(sb.toString(), '|', false, '"', 0, -1, 0);
					emitRow(info);
				}
				consumer.commitAsync();
			}
		} catch (Exception e) {
			// log.error("Unexpected error", e);
		} finally {
			try {
				consumer.commitSync();
			} finally {
				consumer.close();
			}
		}

	}

	/**
	 * @param args
	 * @throws UDRException
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException, UDRException {
		// TODO Auto-generated method stub
		// input params should be
		// 1. connectionString
		// 2. groupId
		// 3. topic
		// 4. number of rows to Read
		// 5. schema
		// 6. table column mapping file
		// 7. input textfile
		if (args == null || args.length < 7) {
			System.out.println("Required params less than 7 ");
			System.out.println("Usage 1. ConnectionString, 2. GroupId, 3. Topic, ");
			System.out.println("4. Number of Rows to read, 5. Schema, 6. Table Column Mapping File,");
			System.out.println("7. test input file");
			System.exit(0);
		}

		InputParameters input = new InputParameters(args[0], Integer.parseInt(args[1]), args[2], args[3], args[4],
				Integer.parseInt(args[5]));
		TypeDecoder decoder = new TypeDecoder(input.getSchemaLocation(), input.getFieldColumnMappingLocation());
		Map<String, TypeInfo> columnMap = decoder.getColumnNameDatatypeMap();

		for (Map.Entry<String, TypeInfo> column : columnMap.entrySet()) {
			System.out.println(column.getKey() + "\t" + column.getValue().getSQLType());
		}

		try (BufferedReader br = new BufferedReader(new FileReader(args[6]))) {

			String sCurrentLine;
			ObjectMapper mapper = new ObjectMapper();
			JsonNode rootNode = null;
			Map<String, String> fieldNameColumnMap = decoder.getFieldColumnNameMap();
			Map<String, Schema.Type> fieldJsonTypeMap = decoder.getFieldJsonTypeMap();
			String fieldName = null;
			StringBuilder sb = new StringBuilder(512);
			JsonNode tempNode = null;
			while ((sCurrentLine = br.readLine()) != null) {

				System.out.println(sCurrentLine);
				rootNode = mapper.readTree(sCurrentLine);
				sb = new StringBuilder(512);
				for (Map.Entry<String, String> fieldColumnEntry : fieldNameColumnMap.entrySet()) {
					fieldName = fieldColumnEntry.getKey();
					tempNode = rootNode.get(fieldName);
					if (tempNode != null) {
						switch (fieldJsonTypeMap.get(fieldName)) {
						case ARRAY:
						case BOOLEAN:
						case BYTES:
						case ENUM:
						case FIXED:
						case MAP:
						case NULL:
						case RECORD:
						case UNION:
							throw new Exception("Not supported type");
						case DOUBLE:
						case FLOAT:
							sb.append(tempNode.getDoubleValue()).append("|");
							break;
						case INT:
							sb.append(tempNode.getIntValue()).append("|");
							break;
						case LONG:
							sb.append(tempNode.getLongValue()).append("|");
							break;
						case STRING:
							sb.append(tempNode.getTextValue()).append("|");
							break;
						default:
							throw new Exception("Not supported type");
						}
					} else {
						sb.append("|");
					}
				}
				System.out.println(sb.toString());
			}
		} catch (Exception e) {
			// log.error("Unexpected error", e);
		}

	}
}

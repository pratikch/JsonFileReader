
import org.trafodion.sql.udr.TypeInfo;
import org.trafodion.sql.udr.UDRException;
import org.trafodion.sql.udr.UDRInvocationInfo;

public class InputParameters {
	private String connectionString = null;
	private int groupId = 0;
	private String topicString = null;
	private String schemaLocation = null;
	private String fieldColumnMappingLocation = null;
	private int numOfRowsToRead = -1;

	/**
	 * 
	 * @param con
	 * @param groupId
	 * @param topic
	 * @param sLocation
	 * @param mapLocation
	 * @param rowsToread
	 */
	public InputParameters(String con, int groupId, String topic, String sLocation, String mapLocation,
			int rowsToread) {
		this.connectionString = con;
		this.groupId = groupId;
		this.topicString = topic;
		this.schemaLocation = sLocation;
		this.fieldColumnMappingLocation = mapLocation;
		this.numOfRowsToRead = rowsToread;
	}

	public String getConnectionString() {
		return connectionString;
	}

	public int getGroupId() {
		return groupId;
	}

	public String getTopicString() {
		return topicString;
	}

	public String getSchemaLocation() {
		return schemaLocation;
	}

	public String getFieldColumnMappingLocation() {
		return fieldColumnMappingLocation;
	}

	public int getNumOfRowsToRead() {
		return numOfRowsToRead;
	}

	/**
	 * 
	 * @param info
	 * @return
	 * @throws UDRException
	 */
	public static InputParameters parseInputParameters(UDRInvocationInfo info) throws UDRException {
		String connectionString = null;
		int groupId = 0;
		String topic = null;
		int numRowsToRead = -1;
		String schemaLocation = null;
		String fieldColumnMapLocation = null;

		int numInputParams = info.par().getNumColumns();

		if (info.getCallPhase() == UDRInvocationInfo.CallPhase.COMPILER_INITIAL_CALL) {
			if (numInputParams < 6) {
				throw new UDRException(38010, "Expecting at least 6 parameters.");
			}

			if (info.par().getColumn(0).getType().getSQLTypeClass() != TypeInfo.SQLTypeClassCode.CHARACTER_TYPE) {
				throw new UDRException(38020, "Expecting a characters for connection string as first argument.");
			}

			if (info.par().getColumn(1).getType().getSQLTypeClass() != TypeInfo.SQLTypeClassCode.NUMERIC_TYPE) {
				throw new UDRException(38030, "Expecting a number for group Id as second argument.");
			}
			if (info.par().getColumn(2).getType().getSQLTypeClass() != TypeInfo.SQLTypeClassCode.CHARACTER_TYPE) {
				throw new UDRException(38040, "Expecting a character for topic as third argument.");
			}
			if (info.par().getColumn(3).getType().getSQLTypeClass() != TypeInfo.SQLTypeClassCode.NUMERIC_TYPE) {
				throw new UDRException(38050, "Expecting a number as Number of rows to read for fourth argument");
			}
			if (info.par().getColumn(4).getType().getSQLTypeClass() != TypeInfo.SQLTypeClassCode.CHARACTER_TYPE) {
				throw new UDRException(38060, "Expecting a file name/location for fifth argument");
			}
			if (info.par().getColumn(5).getType().getSQLTypeClass() != TypeInfo.SQLTypeClassCode.CHARACTER_TYPE) {
				throw new UDRException(38060, "Expecting a field column file mapping location as sixth argument");
			}
		}

		// assign input parameters to local variables
		for (int i = 0; i < numInputParams; i++) {
			if (info.par().isAvailable(i)) {
				switch (i) {
				case 0:
					connectionString = info.par().getString(i);
					break;
				case 1:
					groupId = info.par().getInt(i);
					break;
				case 2:
					topic = info.par().getString(i);
					break;
				case 3:
					numRowsToRead = info.par().getInt(i);
					break;
				case 4:
					schemaLocation = info.par().getString(i);
					break;
				case 5:
					fieldColumnMapLocation = info.par().getString(i);
					break;
				default:
					throw new UDRException(38070, "More or less than 6 arguments provided.");
				}
			} else {
				throw new UDRException(38080, "Parameter number %d must be a literal.", i);
			}
		}

		return new InputParameters(connectionString, groupId, topic, schemaLocation, fieldColumnMapLocation,
				numRowsToRead);
	}

}

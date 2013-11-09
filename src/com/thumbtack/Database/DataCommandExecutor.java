package com.thumbtack.Database;

/**
 * Created with IntelliJ IDEA.
 * User: rakeshkumar
 * Date: 11/9/13
 * Time: 10:26 AM
 * To change this template use File | Settings | File Templates.
 */
class DataCommandExecutor {

	SimpleDatabase database;


	public DataCommandExecutor(SimpleDatabase simpleDatabase) {
		database=simpleDatabase;
	}

	boolean execute(Command cc, String[] commandAndParams) {
		switch(cc) {
			case GET:
				System.out.println(database.getValue(commandAndParams[Constants.VARIABLE_INDEX]));
				break;

			case SET:
				Integer newValue=Integer.parseInt(commandAndParams[Constants.VALUE_INDEX]);

				if(database.getValue(commandAndParams[Constants.VARIABLE_INDEX])!=null) {
					Integer oldValue= database.getValue(commandAndParams[Constants.VARIABLE_INDEX]);
					if(oldValue!=newValue) {
						database.setValueCount(oldValue,database.getValueCount(oldValue)-1);
						setNewValueCount(commandAndParams, newValue);
					}
				}else {
					setNewValueCount(commandAndParams, newValue);
				}
				break;

			case UNSET:
				Integer value=database.getValue(commandAndParams[Constants.VARIABLE_INDEX]);
				if(value!=null) {
					database.setValueCount(value,database.getValueCount(value)-1);
					database.setVariable(commandAndParams[Constants.VARIABLE_INDEX],null);
				}
				break;

			case NUMEQUALTO:
				System.out.println(database.getValueCount(Integer.parseInt(commandAndParams[Constants.VARIABLE_INDEX])));
				break;

			case END:
				return false;

			default:
				System.out.println(Constants.NO_TRANSACTION);

		}
		return true;
	}

	private void setNewValueCount(String[] commandAndParams, Integer newValue) {
		database.setVariable(commandAndParams[Constants.VARIABLE_INDEX],newValue);
		if(database.getValueCount(newValue)!=null) {
			database.setValueCount(newValue,database.getValueCount(newValue)+1);
		}else {
			database.setValueCount(newValue,1);
		}
	}
}

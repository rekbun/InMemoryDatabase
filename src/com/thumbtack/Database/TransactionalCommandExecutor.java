package com.thumbtack.Database;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: rakeshkumar
 * Date: 11/9/13
 * Time: 10:27 AM
 * To change this template use File | Settings | File Templates.
 */

/**
 * data structure for keeping track of transaction and integer value i.e. value count/ value 
*/
class DataSet {
	Integer transactionNumber;
	Integer value;

	DataSet(Integer transactionCount,Integer v) {
		transactionNumber =transactionCount;
		value=v;
	}
}

class TransactionalCommandExecutor {
	HashMap<String,ArrayList<DataSet>> svlMap;      //map for keeping variable values
	HashMap<Integer,ArrayList<DataSet>> vclMap;     //map for storing value counts

	SimpleDatabase database;

	int transactionCount;

	public TransactionalCommandExecutor(SimpleDatabase simpleDatabase) {
		svlMap=new HashMap<String, ArrayList<DataSet>>();
		vclMap=new HashMap<Integer, ArrayList<DataSet>>();
		database=simpleDatabase;
		transactionCount=0;
	}

	public boolean execute(Command cc, String[] commandAndParams) {
		ArrayList<DataSet> valueList;
		switch(cc) {

			case BEGIN:
				/*if(transactionCount==0) {
					for(Map.Entry<Integer, Integer> kv: database.vcMap.entrySet()) {
						ArrayList<DataSet> dataSets=new ArrayList<DataSet>();
						dataSets.add(new DataSet(0,kv.getValue()));
						vclMap.put(kv.getKey(),dataSets);
					}
				}*/
				transactionCount++;
				break;

			case GET:
				valueList=svlMap.get(commandAndParams[Constants.VARIABLE_INDEX]);
				if(valueList==null || valueList.size()==0) {
					System.out.println(database.getValue(commandAndParams[Constants.VARIABLE_INDEX]));
				}else {
					System.out.println(valueList.get(valueList.size()-1).value);
				}
				break;

			case SET:
				set(commandAndParams[Constants.VARIABLE_INDEX],Integer.parseInt(commandAndParams[Constants.VALUE_INDEX]));
				break;

			case UNSET:
				set(commandAndParams[Constants.VARIABLE_INDEX],null);
				break;

			case NUMEQUALTO:
				Integer value= Integer.parseInt(commandAndParams[Constants.VARIABLE_INDEX]);
				valueList= vclMap.get(value);
				if(valueList!=null&&valueList.size()!=0) {
					System.out.println(valueList.get(valueList.size()-1).value);
				}else {
					System.out.println(database.getValueCount(value));

				}
				break;

			case END:
				return false;

			case ROLLBACK:
				if(transactionCount==0) {
					printNoTransAction();
				}else if(svlMap.size()>0) {
					rollbackVariablesValues();
					rollbackValueCounts();
				}
				if(transactionCount>0) {
					transactionCount--;
				}
				if(transactionCount<=0) {
					database.TransactionCompleted();
				}
				break;

			case COMMIT:
				if(transactionCount==0) {
					printNoTransAction();
				}
				for(Map.Entry<String, ArrayList<DataSet>> list:svlMap.entrySet()) {
					database.setVariable(list.getKey(),list.getValue().get(list.getValue().size()-1).value);
				}
				transactionCount=0;

				svlMap.clear();
				vclMap.clear();
				database.TransactionCompleted();
				break;
		}
		return true;
	}

	/*
	 rollbacks all the value counts of the current transaction from the map by iterating over it 
	 */
	private void rollbackValueCounts() {
		for(Iterator<Map.Entry<String,ArrayList<DataSet>>> it = svlMap.entrySet().iterator(); it.hasNext(); ) {
			Map.Entry<String, ArrayList<DataSet>> entry = it.next();
			for(int vi=0;vi<entry.getValue().size();vi++) {
				if(entry.getValue().get(vi).transactionNumber ==transactionCount) {
					entry.getValue().remove(vi);
				}
			}
			if(entry.getValue().size()==0) {
				it.remove();
			}
		}
	}

	/*
	 rollbacks all the values of the current transaction from the map by iterating over it
	 */
	private void rollbackVariablesValues() {
		for(Iterator<Map.Entry<Integer,ArrayList<DataSet>>> it = vclMap.entrySet().iterator(); it.hasNext(); ) {
			Map.Entry<Integer, ArrayList<DataSet>> entry = it.next();
			for(int vi=0;vi<entry.getValue().size();vi++) {
				if(entry.getValue().get(vi).transactionNumber ==transactionCount) {
					entry.getValue().remove(vi);
				}
			}
			if(entry.getValue().size()==0) {
				it.remove();
			}
		}
	}

	private void printNoTransAction() {
		System.out.println(Constants.NO_TRANSACTION);
	}

	/*
	set the value and and its count in the transaction map
	 LOGIC: partially explained
	 if [svlMap] contains [name] then
	    if  it is a new transaction then
	        add a new dataset [name,<transactionNumber,value>]
	        update [vclMap] by adding new Block [newValue,<transactionNumber,valueCount+1>]
	            and adding another block [oldValue,<transactionNumber,valueCount-1>]
        else
            update the current values in svl map
     else add a new block

     complexity expected: O(1)
	 */
	
	private void set(String name,Integer newValue) {
		ArrayList<DataSet> listOfValuesForName=svlMap.get(name);
		ArrayList<DataSet> listOfCountForCurrentValue=vclMap.get(newValue);
		Integer oldValue;
		DataSet dataSet;
		ArrayList<DataSet> oldValueCountList;

		if(listOfValuesForName!=null) {
			oldValue=listOfValuesForName.get(listOfValuesForName.size()-1).value;
			oldValueCountList=vclMap.get(oldValue);

			if(listOfValuesForName.size()<transactionCount) {
				dataSet= new DataSet(transactionCount,newValue);
				listOfValuesForName.add(dataSet);

				if(oldValue!=newValue) {
					oldValueCountList.add(new DataSet(transactionCount,oldValueCountList.get(oldValueCountList.size()-1).value-1));
				}

                if(listOfCountForCurrentValue==null) {
	              listOfCountForCurrentValue=new ArrayList<DataSet>();
	              vclMap.put(newValue,listOfCountForCurrentValue);
	            }
	            listOfCountForCurrentValue.add(new DataSet(transactionCount, newValue));
	        }else {

				if(oldValue!=newValue) {
					listOfValuesForName.get(listOfValuesForName.size()-1).value=newValue;
					updateValueCountMap(newValue, listOfCountForCurrentValue, oldValueCountList);
				}
			}
		}else {
			oldValue=database.getValue(name);
			if(oldValue!=null) {
				oldValueCountList = new ArrayList<DataSet>();
				dataSet = new DataSet(transactionCount,database.getValueCount(oldValue)-1);
				oldValueCountList.add(dataSet);
				vclMap.put(oldValue,oldValueCountList);
			}
			listOfValuesForName=new ArrayList<DataSet>();
			listOfValuesForName.add(new DataSet(transactionCount, newValue));
			if(listOfCountForCurrentValue!=null) {
				dataSet=new DataSet(transactionCount,listOfCountForCurrentValue.get(listOfCountForCurrentValue.size()-1).value+1);
				vclMap.get(newValue).add(dataSet);
			}else {
				listOfCountForCurrentValue=new ArrayList<DataSet>();

				addNewDataset(listOfCountForCurrentValue);
				vclMap.put(newValue,listOfCountForCurrentValue);
			}
			svlMap.put(name,listOfValuesForName);
		}
	}

	private void addNewDataset(ArrayList<DataSet> newValueCountList) {
		DataSet dataSet=new DataSet(transactionCount,1);
		newValueCountList.add(dataSet);
	}

	private void updateValueCountMap(Integer newValue, ArrayList<DataSet> newValueCountList, ArrayList<DataSet> oldValueCountList) {
		DataSet set;
		set= oldValueCountList.get(oldValueCountList.size()-1);
		set.value--;
		if(set.value<=0) {
			oldValueCountList.remove(oldValueCountList.size()-1);
		}
		if(newValueCountList!=null) {
			set=newValueCountList.get(newValueCountList.size()-1);
			set.value++;
		}else {
			newValueCountList=new ArrayList<DataSet>();
			newValueCountList.add(new DataSet(transactionCount,1));
			vclMap.put(newValue,newValueCountList);
		}
	}
}

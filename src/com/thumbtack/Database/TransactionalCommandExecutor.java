package com.thumbtack.Database;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: rakeshkumar
 * Date: 11/9/13
 * Time: 10:27 AM
 * To change this template use File | Settings | File Templates.
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
	HashMap<String,ArrayList<DataSet>> svlMap;
	HashMap<Integer,ArrayList<DataSet>> vclMap;
	SimpleDatabase database;
	int transactionCount;

	public TransactionalCommandExecutor(SimpleDatabase simpleDatabase) {
		svlMap=new HashMap<String, ArrayList<DataSet>>();
		vclMap=new HashMap<Integer, ArrayList<DataSet>>();
		database=simpleDatabase;
		transactionCount=0;
	}

	public boolean execute(Commands cc, String[] commandAndParams) {
		ArrayList<DataSet> valueList;
		switch(cc) {

			case BEGIN:
				if(transactionCount==0) {
					for(Map.Entry<Integer, Integer> kv: database.vcMap.entrySet()) {
						ArrayList<DataSet> dataSets=new ArrayList<DataSet>();
						dataSets.add(new DataSet(0,kv.getValue()));
						vclMap.put(kv.getKey(),dataSets);
					}
				}
				transactionCount++;
				break;

			case GET:
				valueList=svlMap.get(commandAndParams[Constants.VARIABLE_NAME_INDEX]);
				if(valueList==null || valueList.size()==0) {
					System.out.println(database.getValue(commandAndParams[Constants.VARIABLE_NAME_INDEX]));
				}else {
					System.out.println(valueList.get(valueList.size()-1).value);
				}
				break;

			case SET:
				set(commandAndParams[Constants.VARIABLE_NAME_INDEX],Integer.parseInt(commandAndParams[Constants.VARIABLE_VALUE_INDEX]));
				break;

			case UNSET:
				set(commandAndParams[Constants.VARIABLE_NAME_INDEX],null);
				break;

			case NUMEQUALTO:
				Integer value= Integer.parseInt(commandAndParams[Constants.VARIABLE_NAME_INDEX]);
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
					transactionCount=0;
				}
				svlMap.clear();
				vclMap.clear();
				database.TransactionCompleted();
				break;
		}
		return true;
	}

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

	private void set(String name,Integer value) {
		Integer newValue=value;

		ArrayList<DataSet> dataSetArrayList=svlMap.get(name);
		ArrayList<DataSet> newValueCountList=vclMap.get(newValue);
		Integer oldValue;
		ArrayList<DataSet> oldValueCountList;

		if(dataSetArrayList!=null) {
			oldValue=dataSetArrayList.get(dataSetArrayList.size()-1).value;
			oldValueCountList=vclMap.get(oldValue);


			if(dataSetArrayList.size()<transactionCount) {
				DataSet dataSet= new DataSet(transactionCount,newValue);
				dataSetArrayList.add(dataSet);
				oldValueCountList.add(new DataSet(transactionCount,oldValueCountList.get(oldValueCountList.size()-1).value-1));
	            if(newValueCountList==null) {
		            newValueCountList=new ArrayList<DataSet>();
	            }
	            newValueCountList.add(new DataSet(transactionCount,newValue));
	        }else {
				if(oldValue!=newValue) {
					updateValueCountMap(newValue, newValueCountList, oldValueCountList);
				}
			}
		}else {
			oldValue=database.getValue(name);
			if(oldValue!=null) {
				oldValueCountList = new ArrayList<DataSet>();
				DataSet set = new DataSet(transactionCount,database.getValueCount(oldValue)-1);
				oldValueCountList.add(set);
				vclMap.put(oldValue,oldValueCountList);
			}
			dataSetArrayList=new ArrayList<DataSet>();
			dataSetArrayList.add(new DataSet(transactionCount,newValue));
			if(newValueCountList!=null) {
				DataSet dataSet=new DataSet(transactionCount,newValueCountList.get(newValueCountList.size()-1).value+1);
				vclMap.get(newValue).add(dataSet);
			}else {
				newValueCountList=new ArrayList<DataSet>();

				addNewDataset(newValueCountList);
				vclMap.put(newValue,newValueCountList);
			}
			svlMap.put(name,dataSetArrayList);
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

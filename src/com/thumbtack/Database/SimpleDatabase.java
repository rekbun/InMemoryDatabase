package com.thumbtack.Database;

import java.io.*;
import java.util.HashMap;

/**
 * Created with IntelliJ IDEA.
 * User: rakeshkumar
 * Date: 10/21/13
 * Time: 8:08 AM
 * To change this template use File | Settings | File Templates.
 */
enum Command {
	GET,
	SET,
	UNSET,
	NUMEQUALTO,
	END,
	BEGIN,
	ROLLBACK,
	COMMIT
}

class Constants {
	public static int VARIABLE_INDEX =1;
	public static int VALUE_INDEX =2;

	public static final String NO_TRANSACTION = "NO TRANSACTION";
}
public class SimpleDatabase {
	private static int COMMAND_INDEX = 0;
    boolean isDataCommand =true;
	private DataCommandExecutor dataCommandExecutor;
	private TransactionalCommandExecutor transactionalCommandExecutor;
	private boolean finished;

	HashMap<String,Integer> svMap;  //variable name and value map
	HashMap<Integer,Integer> vcMap; //value and value count map

	public Integer getValue(String variableName) {
		return svMap.get(variableName);
	}

	public void setVariable(String variable,Integer value) {
		if(value==null) {
			svMap.remove(variable);
		}
		svMap.put(variable,value);
	}

	public Integer getValueCount(Integer value) {
		Integer cnt= vcMap.get(value);
		if(cnt==null) {
			return 0;
		}else {
			return cnt;
		}
	}

	public void setValueCount(Integer value,Integer valueCount) {
		if( valueCount<=0&&vcMap.get(value)!=null && vcMap.get(value)==1) {
			vcMap.remove(value);
		}else {
			vcMap.put(value,valueCount);
		}
	}



	public SimpleDatabase() {
		dataCommandExecutor=new DataCommandExecutor(this);
		transactionalCommandExecutor=new TransactionalCommandExecutor(this);
		svMap=new HashMap<String, Integer>();
		vcMap=new HashMap<Integer, Integer>();
	}
	public void run() throws IOException {
		BufferedReader inputStream = new BufferedReader(new InputStreamReader(System.in));
		PrintStream printStream = System.out;
		while (!finished) {
			String command = inputStream.readLine();
			String[] commandAndParams = command.split(" ");
			Command cc = Command.valueOf(commandAndParams[COMMAND_INDEX]);
			switch (cc) {
				case BEGIN:
					isDataCommand=false;
			}
			if(isDataCommand) {
				if(!dataCommandExecutor.execute(cc,commandAndParams)) {
					return;
				}
			}else {
				if(!transactionalCommandExecutor.execute(cc,commandAndParams)) {
					return;
				}
			}
		}
	}

	public static void main(String[] args) throws IOException {
		SimpleDatabase sol = new SimpleDatabase();
		sol.run();
	}

	public void TransactionCompleted() {
		isDataCommand=true;
	}
}


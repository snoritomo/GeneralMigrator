package com.kikisoftware.migrator.database;

/**
 * 不正データの為、処理対象としないデータが存在したときにthrowする
 * @author kikisoftware
 */
public class IllegalParameterToBeContinuedException extends Exception {
	/**
	 * 
	 */
	private static final long serialVersionUID = 7250574652023572875L;

	/**
	 * @param message ログに出力させる、なぜこのデータが不正だったかが解るようなメッセージ
	 */
	public IllegalParameterToBeContinuedException(String message){
		super(message);
	}
}

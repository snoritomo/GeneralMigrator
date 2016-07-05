package com.kikisoftware.migrator.database;

import java.util.regex.Pattern;

/**
 * 全てのベースクラスとして、完全に共通的な定数を定義
 * @author kikisoftware
 */
public abstract class Consts {
	/** 正常終了コード **/
	public static final int EXIT_CODE_OK = 0;
	/** コンフィグエラー終了コード **/
	public static final int ERR_CODE_CONFIG_SETTING = 1000;
	/** システムエラー終了コード **/
	public static final int ERR_CODE_SYSTEM_ERROR = 1001;
	/** プログラムエラー終了コード **/
	public static final int ERR_CODE_PROGRAM_ERROR = 1002;
	/** サーバーエラー終了コード **/
	public static final int ERR_CODE_SERVER_ERROR = 1003;

	/** システム改行コード **/
	public static final String RET = System.getProperty("line.separator");
	/** タブ文字 **/
	public static final String TAB = "\t";
	/** zipで使用するツリー構造表現パスセパレータ **/
	public static final String ZIP_SEPARATOR = "/";
	/** URL形式クエリストリング部開始セパレータ **/
	public static final String URL_QUERY_SEPARATOR = "?";
	/** URL形式クエリストリングデータセパレータ **/
	public static final String URL_PARAM_SEPARATOR = "&";
	/** URL形式クエリストリングキーバリューセパレータ **/
	public static final String URL_DATA_SEPARATOR = "=";
	/** windowsファイルシステムパスセパレータ**/
	public static final String WINDOWS_FILE_SYSTEM_PATH_SEPARATOR = "\\";
	/** unixファイルシステムパスセパレータ **/
	public static final String UNIX_FILE_SYSTEM_PATH_SEPARATOR = "/";
	/** ファイル拡張子セパレータ **/
	public static final String FILE_EXTENTION_SEPARATOR = ".";
	/** winndowsファイルシステム絶対パス指定判定用の正規表現 **/
	public static final Pattern WINDOWS_FILE_SYSTEM_ABSOLUTE_LOCAL_PATTERN = Pattern.compile("^[a-zA-Z]:\\\\");
	/** winndowsファイルシステムネットーくパス指定判定用の正規表現 **/
	public static final Pattern WINDOWS_FILE_SYSTEM_ABSOLUTE_NETWORK_PATTERN = Pattern.compile("^\\\\\\\\");
	/** URL形式パスセパレータ **/
	public static final String URL_PATH_SEPARATOR = "/";
	/** samba形式パスセパレータ **/
	public static final String SAMBA_PATH_SEPARATOR = "/";
	/** ftp形式パスセパレータ **/
	public static final String FTP_PATH_SEPARATOR = "/";
	/** 改行コード判定用の正規表現文字列 **/
	public static final String LINE_SPLIT_SEPARATOR = "[\\r\\n]";
	/** ローカルホストIPアドレス **/
	public static final String LOCAL_IP_ADDRESS = "127.0.0.1";

	/** １秒分のミリ秒数**/
	public static final int MILLISEC_ONE_SECOND = 1000;
	/** １分分のミリ秒数 **/
	public static final int MILLISEC_ONE_MINUTE = MILLISEC_ONE_SECOND * 60;
	/** １時間分のミリ秒数 **/
	public static final int MILLISEC_ONE_HOUR = MILLISEC_ONE_MINUTE * 60;
	/** １日分のミリ秒数 **/
	public static final int MILLISEC_ONE_DAY = MILLISEC_ONE_HOUR * 24;
	
	/** SQLステータス　デッドロック **/
	public static final String SQL_STATE_DEAD_LOCK = "41000";
	/** SQLステータス　通信エラー **/
	public static final String SQL_STATE_CONNECTION_ERROR = "08S01";
	
	/**
	 * トランザクション処理の方法を定義
	 * @author kikisoftware
	 */
	public static enum TRANSACTION_MODE {
		None,
		ByRecord,
		All;
	}
}

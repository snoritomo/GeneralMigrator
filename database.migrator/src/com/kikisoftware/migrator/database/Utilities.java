package com.kikisoftware.migrator.database;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * 全てのベースクラスとして、完全に共通的なユーティリティ関数を定義
 * @author kikisoftware
 */
public abstract class Utilities extends Consts {
	/** Loggerを定義。代入は実装クラスで行う事 **/
	public static Logger log_;

	private static java.util.ResourceBundle prop;
	private static Map<String, String> setting;

	/**
	 * コンフィグファイルdatabasemigrator.propertiesを再読み込みする
	 */
	public static void reload() {
		java.util.ResourceBundle.clearCache();
		prop = java.util.ResourceBundle.getBundle("databasemigrator");
		setting = new HashMap<String, String>();
	}
	static {
		reload();
	}
	
	/**
	 * ログ出力を行う。Loggerは各実装クラスで代入したものを使う
	 * @param level 出力ログレベル (NotNull)
	 * @param msg 出力するログメッセージ (NullAllowed)
	 */
	public void outLog(Level level, String msg){
		outLog(log_, level, new String[]{msg});
	}
	
	/**
	 * ログ出力を行う。Loggerを指定し、システム全体で同期的に出力する
	 * @param log Logger (NotNull)
	 * @param level 出力ログレベル (NotNull)
	 * @param msg 出力するログメッセージ (NullAllowed)
	 */
	public static synchronized void outLog(Logger log, Level level, String msg){
		outLog(log, level, new String[]{msg});
	}

	/**
	 * 文字列配列を改行区切りでログ出力を行う。Loggerを指定し、システム全体で同期的に出力する
	 * @param log Logger (NotNull)
	 * @param level 出力ログレベル。nullだと出力なし (NullAllowed)
	 * @param msgs 出力するログメッセージ配列 (NotNull)
	 */
	public static synchronized void outLog(Logger log, Level level, String[] msgs){
		String msg = null;
		if(msgs.length==1){
			msg = msgs[0];
		}
		else{
			StringBuilder sb = new StringBuilder();
			for(String s : msgs){
				if(sb.length()>0)sb.append(RET);
				sb.append(s);
			}
			msg = sb.toString();
		}
		if(level==null){

		}
		else if(level.equals(Level.OFF)){
			System.out.println(msg);
		}
		else if(level.equals(Level.DEBUG)){
			log.debug(msg);
		}
		else if(level.equals(Level.INFO)){
			log.info(msg);
		}
		else if(level.equals(Level.WARN)){
			log.warn(msg);
		}
		else if(level.equals(Level.ERROR)){
			log.error(msg);
		}
		else if(level.equals(Level.FATAL)){
			log.fatal(msg);
		}
		else{
			System.out.println("["+level.toString()+"]" + msg);
			log.fatal(msg);
		}
	}

	/**
	 * スタックトレース情報をログ出力用改行区切り文字列として取り出す
	 * @param t throwされた実体 (NotNull)
	 * @return 取り出されたStackTrace文字列
	 */
	public static String getStackTrace(Throwable t) {
		StringWriter writer = new StringWriter();
		t.printStackTrace(new PrintWriter(writer));
		return writer.toString();
	}

	/**
	 * スタックトレース情報をログ出力用改行区切り文字列として取り出す
	 * @param e throwされた実体 (NotNull)
	 * @return 取り出されたStackTrace文字列
	 */
	public static String getStackTrace(Exception e) {
		return getStackTrace((Throwable)e);
	}

	/**
	 * テキストファイルを指定のエンコードで読み取り、文字列として返す
	 * @param path ファイルパス (NotNull)
	 * @param enc エンコード形式 (NotNull)
	 * @return ファイル内文字列
	 * @throws Exception 基本はIOExceptionだが、NullPointerもあり得る
	 */
	public static String getFileContents(String path, String enc) throws Exception{
		return new String(Files.readAllBytes(Paths.get(path)), enc);
		/*File f = new File(path);
		FileInputStream fi = null;
		StringBuilder sb = new StringBuilder();
		String re = null;
		try {
			fi = new FileInputStream(f);
			byte[] b = new byte[getFileBuffer()];
			int size = 0;
			while((size = fi.read(b)) > 0){
				outLog(log_, Level.DEBUG, " file read size:"+size);
				sb.append(new String(b, enc));
			}
			re = sb.toString();
		} catch (Exception e) {
			// ログ出力
			outLog(log_, Level.FATAL, e.getMessage() + Const.RET + getStackTrace(e));
		}
		finally{
			try {
				fi.close();
			} catch (IOException e) {
			}
		}
		return re;*/
	}

	/**
	 * コンフィグファイルから指定のキーの値を指定の置換を行って取得する。
	 * @param key コンフィグキー (NotNull)
	 * @param pre 置換対象文字列 (NotNull)
	 * @param aft 置換後文字列 (NotNull)
	 * @param replace 置換を行う場合true
	 * @return コンフィグのキーに対応するバリュー値
	 */
	public static String getResourceString(String key, String pre, String aft, boolean replace){
		if(setting.containsKey(key))return setting.get(key);
		String wk = "";
		try {
			wk = prop.getString(key);
			if(replace && pre!=null)wk = wk.replace(pre, aft);
		} catch (Exception e) {
		}
		setting.put(key, wk);
		return wk;
	}
	
	/**
	 * コンフィグファイルから指定のキーの値を取得する。
	 * @param key コンフィグキー (NotNull)
	 * @return コンフィグのキーに対応するバリュー値
	 */
	public static String getResourceString(String key) {
		return getResourceString(key, null, Level.OFF);
	}
	
	/**
	 * コンフィグファイルから指定のキーの値を、デフォルト値となかった場合のログレベルを指定して取得する。
	 * @param key key コンフィグキー (NotNull)
	 * @param def デフォルト値 (NullAllowed)
	 * @param level ログレベル (NullAllowed)
	 * @return コンフィグのキーに対応するバリュー値
	 */
	public static String getResourceString(String key, String def, Level level){
		String wk = getResourceString(key, null, "", false);
		if(wk==null || wk.equals("")){
			if(level!=null && level.equals(Level.FATAL)){
				outLog(log_, level, key+" setting is illegal ["+wk+"].");
				System.exit(ERR_CODE_CONFIG_SETTING);
			}
			else{
				outLog(log_, level, key+" setting is illegal ["+wk+"]. useing default value ["+def+"]");
			}
			wk = def;
			setting.put(key, def);
		}
		return wk;
	}
}

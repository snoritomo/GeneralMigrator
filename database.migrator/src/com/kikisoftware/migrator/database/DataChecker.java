package com.kikisoftware.migrator.database;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Level;

/**
 * 移行データチェックのベースクラス。データチェックの基本とIO部を実装している。
 * このクラスを継承してabstractを実装するだけでデータチェックが行える。
 * @author kikisoftware
 */
public abstract class DataChecker extends Utilities implements Runnable {
	/** コンフィグファイルfile.bufferの値を取得する。コンフィグの指定は必須
	@return 設定されたバッファバイト数 **/
	public static int getFileBuffer() {return Integer.parseInt(getResourceString("file.buffer", "", Level.FATAL));}
	/** コンフィグファイルfile.encodeの値を取得する。コンフィグの指定は必須
	@return 設定された文字コード **/
	public static String getFileEncode() {return getResourceString("file.encode", "", Level.FATAL);}
	/** コンフィグファイルcheck.selectSourceChunkSizeの値を取得する。コンフィグの指定は必須
	@return 設定された移行元の読み込みチャンク数 **/
	public static int getCheckSelectSourceChunkSize() {return Integer.parseInt(getResourceString("check.selectSourceChunkSize", "", Level.FATAL));}
	/** コンフィグファイルcheck.selectDestinationChunkSizeの値を取得する。コンフィグの指定は必須
	@return 設定された移行先の読み込みチャンク数 **/
	public static int getCheckSelectDestinationChunkSize() {return Integer.parseInt(getResourceString("check.selectDestinationChunkSize", "", Level.FATAL));}

	/** 処理数取得SQLを指定した場合は処理数文字列が入る **/
	protected String cnt = null;
	/** 処理数取得SQLを指定した場合は処理数が入る **/
	protected Integer maxcnt = 0;
	/** 現在の処理数 **/
	protected int procNum = 0;
	/** 処理数ログに含める文字列。実装側で指定可能 **/
	protected String countLogAddComment = "";

	/**
	 * コンストラクタ。
	 * ライブラリの読み込みを行うため、継承先は必ず呼び出す事
	 * @throws Exception ライブラリが読み込めなかった時にthrowされる
	 */
	public DataChecker() throws Exception{
		try {
			Class.forName ("com.mysql.jdbc.Driver");
		} catch (Exception e) {
			outLog(log_, Level.FATAL, e.getMessage() + RET + getStackTrace(e));
			throw e;
		}
	}
	
	/**
	 * 処理数取得SQLを返すように実装すると、処理数がロギングされる。
	 * @return 処理数取得SQL (NullAllowed)
	 */
	protected abstract String getCheckCountSql();

	/**
	 * 元データ取得用SQLを保存したファイルパスを返すように実装する。
	 * @return 元データ取得用SQLを保存したファイルパス
	 */
	protected abstract String getSelectSourceFilePath();

	/**
	 * 移行先データ取得用SQLを保存したファイルパスを返すように実装する。
	 * @return 移行先データ取得用SQLを保存したファイルパス
	 */
	protected abstract String getSelectDestinationFilePath();
	
	/**
	 * 元データ側のコネクションを取得し、返す
	 * @return 元データ側DBとのコネクション
	 * @throws SQLException SQLException DBエラー
	 */
	protected abstract Connection getCheckSourceConnection() throws SQLException;

	/**
	 * データ移行先のコネクションを取得し、返す
	 * @return データ移行先DBとのコネクション
	 * @throws SQLException DBエラー
	 */
	protected abstract Connection getCheckDestinationConnection() throws SQLException;
	
	/**
	 * 移行先データの取得準備を行う。
	 * 元データから移行先の対になるデータを取得するように実装する。
	 * ※ここで、チェックしたいデータを取得しておく。
	 * @param srs 今回のループの元データ側レコード
	 * @param ps 現在準備されたデータ取得ステートメント
	 * @throws SQLException DBエラー
	 * @throws IllegalParameterToBeContinuedException 問題のあるデータだった場合に、実装者がthrowする
	 */
	protected abstract void prepareSelect(ResultSet srs, PreparedStatement ps) throws SQLException, IllegalParameterToBeContinuedException;
	
	/**
	 * データのチェックを実装する。
	 * 問題があれば、IllegalParameterToBeContinuedExceptionをthrowする。
	 * @param drs 移行先側レコード
	 * @throws SQLException DBエラー
	 * @throws IllegalParameterToBeContinuedException 問題のあるデータだった場合に、実装者がthrowする
	 */
	protected abstract void checkDatas(ResultSet drs) throws SQLException, IllegalParameterToBeContinuedException;
	
	/**
	 * ログ出力用。元データの特定が可能な値を出力するように実装する
	 * prepareSelect()を呼び出された後に利用可能になる想定
	 * @return 移行元データに一意な文字列
	 */
	protected abstract String getIdentifierSource();
	
	/**
	 * ログ出力用。移行先データの特定が可能な値を出力するように実装する
	 * checkDatas()を呼び出された後に利用可能になる想定
	 * @return 移行先データに一意な文字列
	 */
	protected abstract String getIdentifierDestination();

	/**
	 * select実行
	 * @param ps 現在準備されたデータ取得ステートメント
	 * @return 取得結果レコード
	 * @throws SQLException DBエラー
	 */
	protected ResultSet doSelect(PreparedStatement ps) throws SQLException{
		ResultSet drs = ps.executeQuery();
		ps.clearParameters();
		return drs;
	}
	
	/**
	 * データチェック処理の実態。
	 * main側では各実装クラスのrunを実行するように処理を書く。
	 */
	@Override
	public void run() {
		// ログ出力
		outLog(log_, Level.INFO, "************ チェック開始 *************");

		// データベースの指定
		Connection scon = null;
		try {
			// データベースとの接続
			scon = getCheckSourceConnection();
			// ログ出力
			outLog(log_, Level.INFO, "チェック元データベース接続完了");
		} catch (SQLException e) {
			// ログ出力
			outLog(log_, Level.FATAL, e.getSQLState() + ":" + e.getMessage() + RET + getStackTrace(e));
		} catch (Exception e) {
			// ログ出力
			outLog(log_, Level.FATAL, e.getMessage() + RET + getStackTrace(e));
		}
		if(scon==null){
			outLog(log_, Level.FATAL, "チェック元コネクションが取得できませんでした");
			return;
		}

		// データベースの指定
		Connection dcon = null;
		try {
			// データベースとの接続
			dcon = getCheckDestinationConnection();
			// ログ出力
			outLog(log_, Level.INFO, "チェック先データベース接続完了");
		} catch (SQLException e) {
			// ログ出力
			outLog(log_, Level.FATAL, e.getSQLState() + ":" + e.getMessage() + RET + getStackTrace(e));
		} catch (Exception e) {
			// ログ出力
			outLog(log_, Level.FATAL, e.getMessage() + RET + getStackTrace(e));
		}
		if(dcon==null){
			outLog(log_, Level.FATAL, "チェック先コネクションが取得できませんでした");
			return;
		}

		String sql;
		sql = getCheckCountSql();

		if(sql!=null && !sql.equals("")){
			try(Statement stmt = scon.createStatement()){
				outLog(log_, Level.INFO, "チェック件数取得ステートメント取得完了");
				// SQL 実行
				try(ResultSet rs = stmt.executeQuery(sql)){
					// 結果を出力
					rs.next();
					cnt = rs.getString("cnt");
				}
				maxcnt = Integer.parseInt(cnt);
				outLog(log_, Level.INFO, "チェック件数取得完了");
			} catch (SQLException e) {
				// ログ出力
				outLog(log_, Level.FATAL, e.getSQLState() + ":" + e.getMessage() + RET + getStackTrace(e));
			} catch (Exception e) {
				// ログ出力
				outLog(log_, Level.FATAL, e.getMessage() + RET + getStackTrace(e));
			}
			if(cnt == null || cnt.equals("")){
				outLog(log_, Level.ERROR, "チェック件数取得に失敗しました。" + RET + sql);
				return;
			}
			if(maxcnt==0){
				outLog(log_, Level.INFO, "対象が存在しませんでした。" + RET + sql);
				return;
			}
			// ログ出力
			outLog(log_, Level.INFO, "移行元テーブル件数取得=["+cnt+"]");
		}

		// source select実行
		String sexecsql = null;
		try{
			sexecsql = getFileContents(getSelectSourceFilePath(), getFileEncode());
			outLog(log_, Level.DEBUG, "チェック元SQL:"+sexecsql);
		} catch (Exception e) {
			// ログ出力
			outLog(log_, Level.FATAL, e.getMessage() + RET + getStackTrace(e));
		}
		if(sexecsql == null){
			outLog(log_, Level.ERROR, "チェック元SQLファイルを読み込めませんでした。" + RET + sql);
			return;
		}

		// destination select実行
		String dexecsql = null;
		try{
			dexecsql = getFileContents(getSelectDestinationFilePath(), getFileEncode());
			outLog(log_, Level.DEBUG, "比較対象SQL:"+dexecsql);
		} catch (Exception e) {
			// ログ出力
			outLog(log_, Level.FATAL, e.getMessage() + RET + getStackTrace(e));
		}
		if(dexecsql == null){
			outLog(log_, Level.ERROR, "比較対象SQLファイルを読み込めませんでした。" + getSelectDestinationFilePath());
			return;
		}

		try(Statement sstmt = scon.createStatement()){
			outLog(log_, Level.INFO, "チェック元ステートメント取得完了");
			sstmt.setFetchSize(getCheckSelectSourceChunkSize());
			try(PreparedStatement dps = dcon.prepareStatement(dexecsql)){
				outLog(log_, Level.INFO, "比較対象ステートメント取得完了");
				dps.setFetchSize(getCheckSelectDestinationChunkSize());
				// SQL 実行
				try(ResultSet srs = sstmt.executeQuery(sexecsql)){
					outLog(log_, Level.INFO, "  ** チェック元データ取得開始 **");
					ResultSetMetaData srsmd= srs.getMetaData();
					StringBuilder ssb = new StringBuilder();
					for (int i = 1; i <= srsmd.getColumnCount(); i++) {
						ssb.append(srsmd.getColumnName(i)+RET);
					}
					outLog(log_, Level.DEBUG, ssb.toString());
						
					while(true){
						procNum++;
						boolean snx = srs.next();
						if(!snx){
							int pnum = procNum-1;
							StringBuilder lastsb = new StringBuilder();
							if(cnt!=null && pnum<maxcnt){
								lastsb.append("チェック件数より処理件数が少ないです。");
							}
							else if(cnt!=null && pnum>maxcnt){
								lastsb.append("チェック件数より処理件数が多いです。");
							}
							else if(cnt!=null && pnum==maxcnt){
								lastsb.append("件数はマッチしていました。");
							}
							lastsb.append("チェックを終了します。");
							outLog(log_, Level.INFO, lastsb.toString());
							break;
						}

						try {
							prepareSelect(srs, dps);
							// SQL 実行
							try(ResultSet drs = doSelect(dps)){
								boolean dnx = drs.next();
								if(!dnx){
									outLog(log_, Level.ERROR, "比較対象が存在しませんでした。 "+(procNum)+countLogAddComment+(cnt==null ? "" : " / "+maxcnt)+ " srcid:"+getIdentifierSource());
									continue;
								}
								checkDatas(drs);
								outLog(log_, Level.INFO, "チェックOK "+(procNum)+countLogAddComment+(cnt==null ? "" : " / "+maxcnt)+ " srcid:"+getIdentifierSource()+" destid:"+getIdentifierDestination());
							}
						} catch (IllegalParameterToBeContinuedException e) {
							outLog(log_, Level.ERROR, e.getMessage());
							continue;
						} catch (SQLException e) {
							String sqlstate = e.getSQLState();
							if(sqlstate!= null && sqlstate.equals("08S01")){
								outLog(log_, Level.FATAL, "Exit because connection has broken. SQLState:"+sqlstate+" ERROR Code:"+e.getErrorCode()+" "+(procNum)+countLogAddComment+(cnt==null ? "" : " / "+maxcnt)+": srcid:"+getIdentifierSource()+" dstid:"+getIdentifierDestination()+" "+e.getMessage());
								outLog(log_, Level.DEBUG, getStackTrace(e));
								break;
							}
							else{
								outLog(log_, Level.ERROR, ""+(procNum)+countLogAddComment+(cnt==null ? "" : " / "+maxcnt)+ " SQLState:"+sqlstate+" ERROR Code:"+e.getErrorCode()+": srcid:"+getIdentifierSource()+" dstid:"+getIdentifierDestination()+" "+e.getMessage());
								outLog(log_, Level.DEBUG, getStackTrace(e));
							}
						} catch (Exception e) {
							outLog(log_, Level.ERROR, ""+(procNum)+countLogAddComment+(cnt==null ? "" : " / "+maxcnt)+ " srcid:"+getIdentifierSource()+" dstid:"+getIdentifierDestination()+" "+e.getClass().getName()+" "+e.getMessage());
							outLog(log_, Level.DEBUG, getStackTrace(e));
						}
						finally{
							countLogAddComment = "";
						}
					}
				}
			}
		} catch (SQLException e) {
			outLog(log_, Level.FATAL, e.getSQLState() + ":" + e.getMessage() + RET + getStackTrace(e));
		} catch (Exception e) {
			outLog(log_, Level.FATAL, e.getMessage() + RET + getStackTrace(e));
		} finally {
			try{
				// データベースのクローズ
				if(scon!=null)scon.close();
				if(dcon!=null)dcon.close();
			} catch (SQLException e) {
				// ログ出力
				outLog(log_, Level.WARN, e.getSQLState() + ":" + e.getMessage() + RET + getStackTrace(e));
			} catch (Exception e) {
				// ログ出力
				outLog(log_, Level.WARN, e.getMessage() + RET + getStackTrace(e));
			}
		}

		// ログ出力
		outLog(log_, Level.INFO, "************** チェック終了 ****************");
	}

}

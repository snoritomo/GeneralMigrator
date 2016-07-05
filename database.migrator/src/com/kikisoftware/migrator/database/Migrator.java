package com.kikisoftware.migrator.database;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;

import org.apache.log4j.Level;

/**
 * マイグレーションベースクラス。データの取得と挿入処理を実装している。
 * このクラスを継承してabstractを実装するだけでデータ処理が行える。
 * @author kikisoftware
 */
public abstract class Migrator extends Utilities implements Runnable {
	/** コンフィグファイルfile.bufferの値を取得する。コンフィグの指定は必須
	@return 設定されたバッファバイト数 **/
	public static int getFileBuffer() {return Integer.parseInt(getResourceString("file.buffer", "", Level.FATAL));}
	/** コンフィグファイルfile.encodeの値を取得する。コンフィグの指定は必須
	@return 設定された文字コード **/
	public static String getFileEncode() {return getResourceString("file.encode", "", Level.FATAL);}
	/** コンフィグファイルcheck.selectSourceChunkSizeの値を取得する。コンフィグの指定は必須
	@return 設定された移行元の読み込みチャンク数 **/
	public static int getExecSelectChunkSize() {return Integer.parseInt(getResourceString("exec.selectChunkSize", "", Level.FATAL));}
	/** コンフィグファイルexec.batchChunkSizeの値を取得する。コンフィグの指定は必須
	@return 設定されたバッチ更新サイズ **/
	public static int getExecBatchChunkSize() {return Integer.parseInt(getResourceString("exec.batchChunkSize", "", Level.FATAL));}

	/** バッチinsertを実行する単位 **/
	protected int batchSize = getExecBatchChunkSize();
	/** 処理数取得SQLを指定した場合は処理数文字列が入る **/
	protected String cnt = null;
	/** 処理数取得SQLを指定した場合は処理数が入る **/
	protected Integer maxcnt = 0;
	/** 現在の処理数 **/
	protected int procNum = 0;
	/** 処理数ログに含める文字列。実装側で指定可能 **/
	protected String countLogAddComment = "";
	
	private boolean __skipInsert = false;
	private TRANSACTION_MODE __transactionMode = TRANSACTION_MODE.None;
	
	/**
	 * 今回のループではinsert処理をスキップしたい場合に呼び出す。
	 * 次のループでは自動的に戻っている。
	 */
	protected void skipInsert(){
		__skipInsert = true;
	}
	
	/**
	 * １処理ごとにトランザクションを使用したい時に呼び出す。
	 * コンストラクタで一度だけ呼び出す事。
	 * また、強制的にバッチサイズは1に変更される。
	 */
	protected void setTransactionModeByRecord(){
		__transactionMode = TRANSACTION_MODE.ByRecord;
		batchSize = 1;
	}
	
	/**
	 * バッチ処理全体に対してトランザクションを発行したい時に呼び出す。
	 * コンストラクタで一度だけ呼び出す事。
	 * バッチサイズは設定値に戻される。
	 */
	protected void setTransactionModeAll(){
		__transactionMode = TRANSACTION_MODE.All;
		batchSize = getExecBatchChunkSize();
	}

	/**
	 * トランザクションを無効にする。初期設定の為、通常は呼び出す必要はない。
	 * コンストラクタで一度だけ呼び出す事。
	 * バッチサイズは設定値に戻される。
	 */
	protected void setTransactionModeNone(){
		__transactionMode = TRANSACTION_MODE.None;
		batchSize = getExecBatchChunkSize();
	}
	
	/**
	 * insert処理以外に実行したい処理があればここに記述する。
	 * これはinsert処理前に実行される。
	 * なければ空実装でよい。
	 * @param rs 今回のループで取得したデータ (NotNull)
	 * @param con 移行先のコネクション (NotNull)
	 */
	protected abstract void doOtherProcess(ResultSet rs, Connection con);
	
	/**
	 * 処理数取得SQLを返すように実装すると、処理数がロギングされる。
	 * @return 処理数取得SQL (NullAllowed)
	 */
	protected abstract String getExecSelectCountSql();
	
	/**
	 * 元データ取得用SQLを保存したファイルパスを返すように実装する。
	 * @return 元データ取得用SQLを保存したファイルパス
	 */
	protected abstract String getExecSelectFilePath();
	
	/**
	 * insert実行用SQLを返すように実装する。
	 * PreparedStatementを利用するので文字列内に埋め込み代理文字を入れる事。
	 * ※ただし、insert文に限る必要はない。
	 * @return insert実行用SQL
	 */
	protected abstract String getInsertString();
	
	/**
	 * PreparedStatementに埋め込む値をセットするように実装する。
	 * この処理はループのはじめに呼び出されるため、ここでResultSetから値を取得しておけば、他の処理でも利用できる。
	 * @param rs 今回のループで取得したデータ (NotNull)
	 * @param ps 現在準備されたinsertステートメント
	 * @throws SQLException DBエラー
	 * @throws IllegalParameterToBeContinuedException 問題のあるデータだった場合に処理をスキップしたい時、実装者がthrowする
	 */
	protected abstract void setParameters(ResultSet rs, PreparedStatement ps) throws SQLException, IllegalParameterToBeContinuedException;
	
	/**
	 * 元データ側のコネクションを取得し、返す
	 * @return 元データ側DBとのコネクション
	 * @throws SQLException DBエラー
	 */
	protected abstract Connection getInsertSourceConnection() throws SQLException;
	
	/**
	 * データ移行先のコネクションを取得し、返す
	 * @return データ移行先DBとのコネクション
	 * @throws SQLException DBエラー
	 */
	protected abstract Connection getInsertTargetConnection() throws SQLException;
	
	/**
	 * ログ出力用。データの特定が可能な値を出力するように実装する
	 * setParameters()を呼び出された後に利用可能になる想定
	 * @return データに一意な文字列
	 */
	protected abstract String getIdentifier();
	
	/**
	 * insert実行。
	 * オーバーロードすることでカスタマイズが可能。ただし、ログ出力とバッチインサート管理が実装されているので注意が必要
	 * @param rs 今回のループで取得したデータ (NotNull)
	 * @param ps 現在準備されたinsertステートメント
	 * @throws SQLException DBエラー
	 */
	protected void doInsert(ResultSet rs, PreparedStatement ps) throws SQLException{
		doOtherProcess(rs, getInsertTargetConnection());
		if(batchSize>1){
			if(!__skipInsert)ps.addBatch();
			ps.clearParameters();
			outLog(log_, Level.INFO, "  process:"+(procNum)+(cnt==null ? "" : " / "+maxcnt)+countLogAddComment+" inserting reserved "+getIdentifier());
			if(procNum % batchSize==0 || procNum>=maxcnt){
				ps.executeBatch();
				ps.clearBatch();
				outLog(log_, Level.INFO, "batch executed process:"+(procNum)+(cnt==null ? "" : " / "+maxcnt));
			}
		}
		else{
			if(!__skipInsert)ps.executeUpdate();
			outLog(log_, Level.INFO, "process:"+(procNum)+countLogAddComment+(cnt==null ? "" : " / "+maxcnt)+ " inserted "+getIdentifier());
		}
		__skipInsert = false;
		countLogAddComment = "";
	}
	
	/**
	 * データ移行処理の実態。
	 * main側では各実装クラスのrunを実行するように処理を書く。
	 */
	@Override
	public void run() {
		// ログ出力
		outLog(log_, Level.INFO, "************ 処理開始 *************");

		try {
			Class.forName ("com.mysql.jdbc.Driver");
		} catch (Exception e) {
			outLog(log_, Level.FATAL, e.getMessage() + RET + getStackTrace(e));
			return;
		}

		// データベースの指定
		Connection con = null;
		try {
			// データベースとの接続
			con = getInsertSourceConnection();
			// ログ出力
			outLog(log_, Level.INFO, "移行元データベース接続完了");
		} catch (SQLException e) {
			// ログ出力
			outLog(log_, Level.FATAL, e.getSQLState() + ":" + e.getMessage() + RET + getStackTrace(e));
		} catch (Exception e) {
			// ログ出力
			outLog(log_, Level.FATAL, e.getMessage() + RET + getStackTrace(e));
		}
		if(con==null){
			outLog(log_, Level.FATAL, "移行元コネクションが取得できませんでした");
			return;
		}

		// 初期化
		String sql;
		sql = getExecSelectCountSql();

		if(sql!=null && !sql.equals("")){
			try(Statement stmt = con.createStatement()){
				outLog(log_, Level.INFO, "件数取得ステートメント取得完了");
				// SQL 実行
				try(ResultSet rs = stmt.executeQuery(sql)){
					// 結果を出力
					rs.next();
					cnt = rs.getString("cnt");
				}
				maxcnt = Integer.parseInt(cnt);
				outLog(log_, Level.INFO, "移行元データ数取得完了");
			} catch (SQLException e) {
				// ログ出力
				outLog(log_, Level.FATAL, e.getSQLState() + ":" + e.getMessage() + RET + getStackTrace(e));
			} catch (Exception e) {
				// ログ出力
				outLog(log_, Level.FATAL, e.getMessage() + RET + getStackTrace(e));
			}
			if(cnt == null || cnt.equals("")){
				outLog(log_, Level.ERROR, "件数取得に失敗しました。" + RET + sql);
				return;
			}
			if(maxcnt==0){
				outLog(log_, Level.INFO, "対象が存在しませんでした。" + RET + sql);
				return;
			}
			// ログ出力
			outLog(log_, Level.INFO, "移行元テーブル件数取得=["+cnt+"]");
		}

		// select実行
		String execsql = null;
		try{
			execsql = getFileContents(getExecSelectFilePath(), getFileEncode());
			outLog(log_, Level.DEBUG, "実行SQL:"+execsql);
		} catch (Exception e) {
			// ログ出力
			outLog(log_, Level.FATAL, e.getMessage() + RET + getStackTrace(e));
		}
		if(execsql == null){
			outLog(log_, Level.ERROR, "SQLファイルを読み込めませんでした。" + RET + sql);
			return;
		}

		Connection con_insert_to = null;
		try(Statement stmt = con.createStatement()){
			outLog(log_, Level.INFO, "移行元ステートメント取得完了");
			stmt.setFetchSize(getExecSelectChunkSize());
			// SQL 実行
			try(ResultSet rs = stmt.executeQuery(execsql)){
				outLog(log_, Level.INFO, "  ** データ取得開始 **");
				ResultSetMetaData rsmd= rs.getMetaData();
				StringBuilder sb = new StringBuilder();
				for (int i = 1; i <= rsmd.getColumnCount(); i++) {
					sb.append(rsmd.getColumnName(i)+RET);
				}
				outLog(log_, Level.DEBUG, sb.toString());
			
				String inssql = getInsertString();
				outLog(log_, Level.DEBUG, inssql);
				
				// 登録先データベースとの接続
				con_insert_to = getInsertTargetConnection();
				// ログ出力
				outLog(log_, Level.INFO, "登録先データベース接続完了");
				
				// Mode Noneならオートコミット
				con_insert_to.setAutoCommit(__transactionMode == TRANSACTION_MODE.None);
				
				try(PreparedStatement ps = con_insert_to.prepareStatement(inssql)){
					while(rs.next()){
						procNum++;
						// Mode AllならSavePoint
						Savepoint savepoint = null;
						if(__transactionMode == TRANSACTION_MODE.All)savepoint = con_insert_to.setSavepoint(Integer.toString(procNum));
						boolean sqlDone = false;
						try {
							setParameters(rs, ps);
							doInsert(rs, ps);
							sqlDone = true;
						} catch (IllegalParameterToBeContinuedException e) {
							outLog(log_, Level.WARN, e.getMessage());
						} catch (SQLException e) {
							String sqlstate = e.getSQLState();
							if(sqlstate!= null && sqlstate.equals(SQL_STATE_CONNECTION_ERROR)){
								outLog(log_, Level.FATAL, (procNum)+countLogAddComment+(cnt==null ? "" : " / "+maxcnt)+" Exit because connection has broken. SQLState:"+sqlstate+" ERROR Code:"+e.getErrorCode()+" id:"+getIdentifier()+" "+e.getMessage());
								outLog(log_, Level.DEBUG, getStackTrace(e));
								// 接続断なので処理終了
								break;
							}
							else{
								outLog(log_, Level.ERROR, (procNum)+countLogAddComment+(cnt==null ? "" : " / "+maxcnt)+" SQLState:"+sqlstate+" ERROR Code:"+e.getErrorCode()+ " id:"+getIdentifier()+" "+e.getMessage());
								outLog(log_, Level.DEBUG, getStackTrace(e));
							}
						} catch (Exception e) {
							outLog(log_, Level.ERROR, (procNum)+countLogAddComment+(cnt==null ? "" : " / "+maxcnt)+ " id:"+getIdentifier()+" "+e.getClass().getName()+" "+e.getMessage());
							outLog(log_, Level.DEBUG, getStackTrace(e));
						} finally {
							// コミット・ロールバック処理。失敗するようなら次の処理もどうせ失敗なので、外側のエラー処理に任せる
							if(sqlDone){
								// Mode ByRecordならコミット。AllならSavePointをリリース
								if(__transactionMode == TRANSACTION_MODE.ByRecord)
									con_insert_to.commit();
								else if(__transactionMode == TRANSACTION_MODE.All)
									con_insert_to.releaseSavepoint(savepoint);
							}
							else{
								// Mode ByRecordならロールバック、AllならSavePointへ
								if(__transactionMode == TRANSACTION_MODE.ByRecord)
									con_insert_to.rollback();
								else if(__transactionMode == TRANSACTION_MODE.All)
									con_insert_to.rollback(savepoint);
							}
							ps.clearParameters();
						}
					}
				}
				// Mode Allならコミット
				if(__transactionMode == TRANSACTION_MODE.All)con_insert_to.commit();
			}
		} catch (SQLException e) {
			outLog(log_, Level.FATAL, e.getSQLState() + ":" + e.getMessage() + RET + getStackTrace(e));
		} catch (Exception e) {
			outLog(log_, Level.FATAL, e.getMessage() + RET + getStackTrace(e));
		} finally {
			try{
				// データベースのクローズ
				if(con_insert_to!=null)con_insert_to.close();
				if(con!=null)con.close();
			} catch (SQLException e) {
				// ログ出力
				outLog(log_, Level.WARN, e.getSQLState() + ":" + e.getMessage() + RET + getStackTrace(e));
			} catch (Exception e) {
				// ログ出力
				outLog(log_, Level.WARN, e.getMessage() + RET + getStackTrace(e));
			}
		}

		// ログ出力
		outLog(log_, Level.INFO, "************** 処理終了 ****************");
	}
}

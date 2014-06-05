package com.rishabh.bigdata.log;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.DateFormat;
import java.util.Calendar;
import java.util.Date;

public class Logger {

	private static Logger _me;

	private Calendar cal;
	private Date mDate;
	private DateFormat mDateFormatter;

	private File mErrorLogFile;
	private File mInfoLogFile;
	private FileOutputStream mLogFileStream;
	private String mPath;

	private String mLog;
	private String mLogTime;

	private Logger() throws Exception {
		cal = Calendar.getInstance();
		Integer cal_for_month = cal.get(Calendar.MONTH) + 1;
		Integer cal_for_year = cal.get(Calendar.YEAR);
		Integer cal_for_day = cal.get(Calendar.DAY_OF_MONTH);

		mPath = System.getProperty("user.home")
				+ System.getProperty("file.separator");

		File mLogDirectory = new File(mPath + "BigData-Logs"
				+ System.getProperty("file.separator") + cal_for_day
				+ cal_for_month + cal_for_year);

		if (!mLogDirectory.exists())
			mLogDirectory.mkdirs();

		mErrorLogFile = new File(mLogDirectory.getAbsolutePath()
				+ System.getProperty("file.separator") + "error" + ".log");
		mInfoLogFile = new File(mLogDirectory.getAbsolutePath()
				+ System.getProperty("file.separator") + "info" + ".log");

		if (!mErrorLogFile.exists())
			mErrorLogFile.createNewFile();

		if (!mInfoLogFile.exists())
			mInfoLogFile.createNewFile();
	}

	/*
	 * Gets the logger instance to log errors, information to the local file
	 * system at user home directory
	 */
	public static Logger getInstance() {
		if (_me == null)
			try {
				_me = new Logger();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		return _me;
	}

	/*
	 * Logs errors to the local file system
	 */
	public boolean logError(String mTitle, String mDetails) {
		try {

			mLogFileStream = new FileOutputStream(mErrorLogFile, true);

			mLogTime = cal.getTime().toString();
			
			mLog = System.getProperty("line.separator") + mLogTime + " -> " + mTitle + " -- " + mDetails;
			mLogFileStream.write(mLog.getBytes());

			return true;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return false;
	}

	/*
	 * Logs information to the local file system
	 */
	public boolean logInfo(String mTitle, String mDetails) {
		try {

			mLogFileStream = new FileOutputStream(mInfoLogFile, true);

			mLogTime = cal.getTime().toString();
			
			mLog = System.getProperty("line.separator") + mLogTime + " -> " + mTitle + " -- " + mDetails;
			mLogFileStream.write(mLog.getBytes());

			return true;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return false;
	}
}

package com.victor.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.SimpleTimeZone;
import java.util.TimeZone;

public class TimeUtil {
    /**
     * 获取当天时间，北京时间，格式为yyyyMMdd
     */
    public static String getToday() {
        return stampToBeijingDate(Long.toString(System.currentTimeMillis()), 8);
    }


    /**
     * 获取当天时间，北京时间，格式自定义，如yyyyMMdd和yyyy-MM-dd
     *
     * @param dateFormat
     */
    public static String getTodayByFormat(String dateFormat) {
        return stampToBeijingDateByFormat(Long.toString(System.currentTimeMillis()), 8, dateFormat);
    }

    /**
     * 获取指定时间戳的北京时间，格式自定义如 yyyyMMdd和yyyy-MM-dd
     *
     * @param dateFormat
     */
    public static String getTimeByFormat(long timeMillis, String dateFormat) {
        return stampToBeijingDateByFormat(Long.toString(timeMillis), 8, dateFormat);
    }

    /**
     * 获取当天时间，自定义时区，格式为yyyyMMdd
     *
     * @param timeZoneOffset
     */
    public static String getToday(float timeZoneOffset) {
        return stampToBeijingDate(Long.toString(System.currentTimeMillis()), timeZoneOffset);
    }

    /**
     * 获取当天时间，北京时间,格式为yyyy-MM-dd HH:mm:ss
     *
     * @return
     */
    public static String getToday2Second() {
        return stampToBeijingDate2Second(Long.toString(System.currentTimeMillis()), 8);
    }

    /**
     * 获取当天时间，自定义时区,格式为yyyy-MM-dd HH:mm:ss
     *
     * @param timeZoneOffset
     * @return
     */
    public static String getToday2Second(float timeZoneOffset) {
        return stampToBeijingDate2Second(Long.toString(System.currentTimeMillis()), timeZoneOffset);
    }


    /**
     * 将时间戳转换为北京时间，格式为yyyyMMdd
     * timeZoneOffset原为int类型，为班加罗尔调整成float类型
     * timeZoneOffset表示时区，如中国一般使用东八区，因此timeZoneOffset就是8
     *
     * @return
     */
    public static String stampToBeijingDate(String s, float timeZoneOffset) {
        if (timeZoneOffset > 13 || timeZoneOffset < -12) {
            timeZoneOffset = 0;
        }
        int newTime = (int) (timeZoneOffset * 60 * 60 * 1000);
        TimeZone timeZone;
        String[] ids = TimeZone.getAvailableIDs(newTime);
        if (ids.length == 0) {
            timeZone = TimeZone.getDefault();
        } else {
            timeZone = new SimpleTimeZone(newTime, ids[0]);
        }
        String res;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        sdf.setTimeZone(timeZone);
        long lt = new Long(s);
        Date date = new Date(lt);
        res = sdf.format(date);
        return res;
    }

    /**
     * 将时间戳转换为北京时间，格式由dateFormat定，如yyyyMMdd和yyyy-MM-dd
     * timeZoneOffset原为int类型，为班加罗尔调整成float类型
     * timeZoneOffset表示时区，如中国一般使用东八区，因此timeZoneOffset就是8
     */
    public static String stampToBeijingDateByFormat(String s, float timeZoneOffset, String dateFormat) {
        if (timeZoneOffset > 13 || timeZoneOffset < -12) {
            timeZoneOffset = 0;
        }
        int newTime = (int) (timeZoneOffset * 60 * 60 * 1000);
        TimeZone timeZone;
        String[] ids = TimeZone.getAvailableIDs(newTime);
        if (ids.length == 0) {
            timeZone = TimeZone.getDefault();
        } else {
            timeZone = new SimpleTimeZone(newTime, ids[0]);
        }
        String res;
        SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
        sdf.setTimeZone(timeZone);
        long lt = new Long(s);
        Date date = new Date(lt);
        res = sdf.format(date);
        return res;
    }

    /**
     * 将时间戳转换为北京时间，格式为yyyy-MM-dd HH:mm:ss
     * timeZoneOffset原为int类型，为班加罗尔调整成float类型
     * timeZoneOffset表示时区，如中国一般使用东八区，因此timeZoneOffset就是8
     *
     * @return
     */
    public static String stampToBeijingDate2Second(String s, float timeZoneOffset) {
        if (timeZoneOffset > 13 || timeZoneOffset < -12) {
            timeZoneOffset = 0;
        }
        int newTime = (int) (timeZoneOffset * 60 * 60 * 1000);
        TimeZone timeZone;
        String[] ids = TimeZone.getAvailableIDs(newTime);
        if (ids.length == 0) {
            timeZone = TimeZone.getDefault();
        } else {
            timeZone = new SimpleTimeZone(newTime, ids[0]);
        }
        String res;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        sdf.setTimeZone(timeZone);
        long lt = new Long(s);
        Date date = new Date(lt);
        res = sdf.format(date);
        return res;
    }


    /**
     * 字符串转日期
     *
     * @param strDate
     * @param pattern "yyyy-MM-dd HH:mm:ss"或者其他格式
     * @return 北京时间
     */
    public static Date str2Date(String strDate, String pattern) {
        int newTime = (int) (8 * 60 * 60 * 1000);
        TimeZone timeZone;
        String[] ids = TimeZone.getAvailableIDs(newTime);
        if (ids.length == 0) {
            timeZone = TimeZone.getDefault();
        } else {
            timeZone = new SimpleTimeZone(newTime, ids[0]);
        }
        SimpleDateFormat df = new SimpleDateFormat(pattern);
        df.setTimeZone(timeZone);
        Date date = new Date();
        try {
            date = df.parse(strDate);
            System.out.println(date);
        } catch (ParseException px) {
            px.printStackTrace();
        }
        return date;
    }

    /**
     * 将北京时间转换为时间戳
     * 返回毫秒级时间戳
     */
    public static long dateToStamp(String s) throws ParseException {
        int newTime = (int) (8 * 60 * 60 * 1000);
        TimeZone timeZone;
        String[] ids = TimeZone.getAvailableIDs(newTime);
        if (ids.length == 0) {
            timeZone = TimeZone.getDefault();
        } else {
            timeZone = new SimpleTimeZone(newTime, ids[0]);
        }
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        simpleDateFormat.setTimeZone(timeZone);
        Date date = simpleDateFormat.parse(s);
        return date.getTime();
    }

    /**
     * 判断时间戳是否为今天，单位为毫秒
     *
     * @param time
     * @return
     * @throws ParseException
     */
    public static Boolean isTodayTime(long time) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Date date = new Date(time);
        Date old = sdf.parse(sdf.format(date));
        Date now = sdf.parse(sdf.format(new Date()));
        long oldTime = old.getTime();
        long nowTime = now.getTime();

        long day = (nowTime - oldTime) / (24 * 60 * 60 * 1000);

        if (day < 1) {  //今天
            return true;
        } else {
            return false;
        }
    }
}

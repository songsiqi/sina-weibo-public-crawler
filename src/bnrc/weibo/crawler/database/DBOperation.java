package bnrc.weibo.crawler.database;

import java.sql.PreparedStatement;
import java.util.List;

import org.apache.log4j.Logger;
import bnrc.weibo.crawler.model.*;


/**
 * 数据库操作类
 * @author songsiqi
 *
 */
public class DBOperation {
	
	private static final Logger logger = Logger.getLogger(DBOperation.class);
	
	// 将用户信息插入到users表中，批量插入
	public static void insert2UsersTable(List<UserBean> userBeanList) {
		PreparedStatement pstmt = null;
		ConnectManager cm = null;
		String sql = null;
		
		try {
			do {
				cm = ConnectPool.getConnectPool().getConnectManager();
			} while (cm == null);
			
			sql = "begin tran;";
			sql += "update users set screen_name=?,name=?,province=?,city=?,location=?,description=?,url=?,profile_image_url=?,domain=?,gender=?,followers_count=?,friends_count=?,statuses_count=?,favourites_count=?,created_at=?,following=?,verified=?,verified_type=?,allow_all_act_msg=?,allow_all_comment=?,follow_me=?,avatar_large=?,online_status=?,bi_followers_count=?,remark=?,lang=?,verified_reason=?,weihao=?,geo_enabled=?,statuses_frequency=?,update_time=? where user_id=?;";
			sql += "if @@rowcount = 0 ";
			sql += "insert into users(user_id,screen_name,name,province,city,location,description,url,profile_image_url,domain,gender,followers_count,friends_count,statuses_count,favourites_count,created_at,following,verified,verified_type,allow_all_act_msg,allow_all_comment,follow_me,avatar_large,online_status,bi_followers_count,remark,lang,verified_reason,weihao,geo_enabled,statuses_frequency,update_time) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);";
			sql += "commit tran;";

			pstmt = cm.getConnection().prepareStatement(sql);
			
			for (UserBean userBean : userBeanList) {
				pstmt.setString(1, userBean.getScreenName());
				pstmt.setString(2, userBean.getName());
				pstmt.setString(3, userBean.getProvince());
				pstmt.setString(4, userBean.getCity());
				pstmt.setString(5, userBean.getLocation());
				pstmt.setString(6, userBean.getDescription());
				pstmt.setString(7, userBean.getUrl());
				pstmt.setString(8, userBean.getProfileImageUrl());
				pstmt.setString(9, userBean.getUserDomain());
				pstmt.setString(10, userBean.getGender());
				pstmt.setInt(11, userBean.getFollowersCount());
				pstmt.setInt(12, userBean.getFriendsCount());
				pstmt.setInt(13, userBean.getStatusesCount());
				pstmt.setInt(14, userBean.getFavouritesCount());
				pstmt.setTimestamp(15, userBean.getCreatedAt());
				pstmt.setBoolean(16, userBean.isFollowing());
				pstmt.setBoolean(17, userBean.isVerified());
				pstmt.setInt(18, userBean.getVerifiedType());
				pstmt.setBoolean(19, userBean.isAllowAllActMsg());
				pstmt.setBoolean(20, userBean.isAllowAllComment());
				pstmt.setBoolean(21, userBean.isFollowMe());
				pstmt.setString(22, userBean.getAvatarLarge());
				pstmt.setInt(23, userBean.getOnlineStatus());
				pstmt.setInt(24, userBean.getBiFollowersCount());
				pstmt.setString(25, userBean.getRemark());
				pstmt.setString(26, userBean.getLang());
				pstmt.setString(27, userBean.getVerifiedReason());
				pstmt.setString(28, userBean.getWeihao());
				pstmt.setBoolean(29, userBean.isGeoEnabled());
				pstmt.setDouble(30, userBean.getStatusesFrequency());
				pstmt.setTimestamp(31, userBean.getUpdateTime());
				pstmt.setLong(32, userBean.getUserId());
				
				pstmt.setLong(33, userBean.getUserId());
				pstmt.setString(34, userBean.getScreenName());
				pstmt.setString(35, userBean.getName());
				pstmt.setString(36, userBean.getProvince());
				pstmt.setString(37, userBean.getCity());
				pstmt.setString(38, userBean.getLocation());
				pstmt.setString(39, userBean.getDescription());
				pstmt.setString(40, userBean.getUrl());
				pstmt.setString(41, userBean.getProfileImageUrl());
				pstmt.setString(42, userBean.getUserDomain());
				pstmt.setString(43, userBean.getGender());
				pstmt.setInt(44, userBean.getFollowersCount());
				pstmt.setInt(45, userBean.getFriendsCount());
				pstmt.setInt(46, userBean.getStatusesCount());
				pstmt.setInt(47, userBean.getFavouritesCount());
				pstmt.setTimestamp(48, userBean.getCreatedAt());
				pstmt.setBoolean(49, userBean.isFollowing());
				pstmt.setBoolean(50, userBean.isVerified());
				pstmt.setInt(51, userBean.getVerifiedType());
				pstmt.setBoolean(52, userBean.isAllowAllActMsg());
				pstmt.setBoolean(53, userBean.isAllowAllComment());
				pstmt.setBoolean(54, userBean.isFollowMe());
				pstmt.setString(55, userBean.getAvatarLarge());
				pstmt.setInt(56, userBean.getOnlineStatus());
				pstmt.setInt(57, userBean.getBiFollowersCount());
				pstmt.setString(58, userBean.getRemark());
				pstmt.setString(59, userBean.getLang());
				pstmt.setString(60, userBean.getVerifiedReason());
				pstmt.setString(61, userBean.getWeihao());
				pstmt.setBoolean(62, userBean.isGeoEnabled());
				pstmt.setDouble(63, userBean.getStatusesFrequency());
				pstmt.setTimestamp(64, userBean.getUpdateTime());
				
				pstmt.addBatch();
			}
	    	
	    	pstmt.executeBatch();
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("插入用户信息错误，重新插入！");
			
			// 写入数据库失败，重新调用函数
			insert2UsersTable(userBeanList);
		} finally {
			// 关闭会话，释放连接
			try {
				if (pstmt != null) {
					pstmt.close();
				}
				cm.release();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	// 插入数据到statuses表，批量插入
	public static void insert2StatusesTable(List<StatusBean> statusBeanList) {
		ConnectManager cm = null;
		PreparedStatement pstmt = null;
		String sql = null;
		
		try {
			do {
				cm = ConnectPool.getConnectPool().getConnectManager();
			} while(cm == null);
			
			sql = "begin tran;";
			sql += "update statuses set user_id=?,created_at=?,mid=?,content=?,source_url=?,source_name=?,favorited=?,truncated=?,in_reply_to_status_id=?,in_reply_to_user_id=?,in_reply_to_screen_name=?,thumbnail_pic=?,bmiddle_pic=?,original_pic=?,retweeted_status_id=?,geo_type=?,geo_coordinates_x=?,geo_coordinates_y=?,reposts_count=?,comments_count=?,mlevel=?,iteration=?,update_time=? where status_id=?;";
			sql += "if @@rowcount = 0 ";
			sql += "insert into statuses(status_id,user_id,created_at,mid,content,source_url,source_name,favorited,truncated,in_reply_to_status_id,in_reply_to_user_id,in_reply_to_screen_name,thumbnail_pic,bmiddle_pic,original_pic,retweeted_status_id,geo_type,geo_coordinates_x,geo_coordinates_y,reposts_count,comments_count,mlevel,iteration,update_time) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);";
			sql += "commit tran;";
			
			pstmt = cm.getConnection().prepareStatement(sql);
			
			for (StatusBean statusBean : statusBeanList) {
				pstmt.setLong(1, statusBean.getUserId());
				pstmt.setTimestamp(2, statusBean.getCreatedAt());
				pstmt.setLong(3, statusBean.getMid());
				pstmt.setString(4, statusBean.getContent());
				pstmt.setString(5, statusBean.getSourceUrl());
				pstmt.setString(6, statusBean.getSourceName());
				pstmt.setBoolean(7, statusBean.isFavorited());
				pstmt.setBoolean(8, statusBean.isTruncated());
				pstmt.setLong(9, statusBean.getInReplyToStatusId());
				pstmt.setLong(10, statusBean.getInReplyToUserId());
				pstmt.setString(11, statusBean.getInReplyToScreenName());
				pstmt.setString(12, statusBean.getThumbnailPic());
				pstmt.setString(13, statusBean.getBmiddlePic());
				pstmt.setString(14, statusBean.getOriginalPic());
				pstmt.setLong(15, statusBean.getRetweetedStatusId());
				pstmt.setString(16, statusBean.getGeoType());
				pstmt.setDouble(17, statusBean.getGeoCoordinatesX());
				pstmt.setDouble(18, statusBean.getGeoCoordinatesY());
				pstmt.setInt(19, statusBean.getRepostsCount());
				pstmt.setInt(20, statusBean.getCommentsCount());
				pstmt.setInt(21, statusBean.getMlevel());
				pstmt.setInt(22, statusBean.getIteration());
				pstmt.setTimestamp(23, statusBean.getUpdateTime());
				pstmt.setLong(24, statusBean.getStatusId());
				
				pstmt.setLong(25, statusBean.getStatusId());
				pstmt.setLong(26, statusBean.getUserId());
				pstmt.setTimestamp(27, statusBean.getCreatedAt());
				pstmt.setLong(28, statusBean.getMid());
				pstmt.setString(29, statusBean.getContent());
				pstmt.setString(30, statusBean.getSourceUrl());
				pstmt.setString(31, statusBean.getSourceName());
				pstmt.setBoolean(32, statusBean.isFavorited());
				pstmt.setBoolean(33, statusBean.isTruncated());
				pstmt.setLong(34, statusBean.getInReplyToStatusId());
				pstmt.setLong(35, statusBean.getInReplyToUserId());
				pstmt.setString(36, statusBean.getInReplyToScreenName());
				pstmt.setString(37, statusBean.getThumbnailPic());
				pstmt.setString(38, statusBean.getBmiddlePic());
				pstmt.setString(39, statusBean.getOriginalPic());
				pstmt.setLong(40, statusBean.getRetweetedStatusId());
				pstmt.setString(41, statusBean.getGeoType());
				pstmt.setDouble(42, statusBean.getGeoCoordinatesX());
				pstmt.setDouble(43, statusBean.getGeoCoordinatesY());
				pstmt.setInt(44, statusBean.getRepostsCount());
				pstmt.setInt(45, statusBean.getCommentsCount());
				pstmt.setInt(46, statusBean.getMlevel());
				pstmt.setInt(47, statusBean.getIteration());
				pstmt.setTimestamp(48, statusBean.getUpdateTime());
				
				pstmt.addBatch();
			}
			
			pstmt.executeBatch();
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("插入用户微博错误，重新插入！");
			
			// 写入数据库失败，重新调用函数
			insert2StatusesTable(statusBeanList);
		} finally {
			// 关闭会话，释放连接
			try {
				if (pstmt != null) {
					pstmt.close();
				}
				cm.release();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
}

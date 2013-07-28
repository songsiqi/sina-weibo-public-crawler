package bnrc.weibo.crawler.util;

import java.util.List;

import org.apache.log4j.Logger;

import weibo4j.Timeline;
import weibo4j.model.Status;
import weibo4j.model.WeiboException;

public class WeiboInterImpl implements WeiboInter {
	
	private Timeline timeline = new Timeline();
	
	private static final Logger logger = Logger.getLogger(WeiboInterImpl.class);
	
	@Override
	public List<Status> getPublicStatus() {
		
		List<Status> statusList = null;
		
		try {
			statusList = timeline.getPublicTimeline(200, 0).getStatuses();
		} catch (Exception e) {
			logger.error("爬取公共微博过程中出错");
			if (e instanceof WeiboException) {
				int errorCode = ((WeiboException) e).getErrorCode();
				String errorInfo = ((WeiboException) e).getError();
				logger.error("Error Code: " + errorCode + " : " + errorInfo);
				
				if (errorCode == 10023) { // 用户请求频次超过上限
					logger.error("用户请求频次超过上限");
					AccessToken.setOneAccessToken();
					return getPublicStatus();
				} else if (errorCode == 10022) { // IP请求频次超过上限
					logger.error("IP请求频次超过上限");
					try {
						Thread.sleep(1000 * 60 * 30);
					} catch (InterruptedException e1) {
						e1.printStackTrace();
					}
					return getPublicStatus();
				} else if (errorCode == -1) { // 新浪微博API提供数据出错
					logger.error("新浪微博API提供数据出错");
					try {
						Thread.sleep(1000 * 60 * 1);
					} catch (InterruptedException e1) {
						e1.printStackTrace();
					}
					return getPublicStatus();
				}
			} else {
				e.printStackTrace();
			}
		}
		
		return statusList;
	}
	
}

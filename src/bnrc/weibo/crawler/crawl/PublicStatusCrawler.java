package bnrc.weibo.crawler.crawl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import weibo4j.model.Status;
import bnrc.weibo.crawler.database.DBOperation;
import bnrc.weibo.crawler.model.StatusBean;
import bnrc.weibo.crawler.model.UserBean;
import bnrc.weibo.crawler.util.WeiboInterImpl;

/**
 * @author songsiqi
 *
 */
public class PublicStatusCrawler {
	
	private WeiboInterImpl weiboInterImpl = new WeiboInterImpl();
	
	private static final Logger logger = Logger.getLogger(PublicStatusCrawler.class);
	
	// 爬取用户的微博信息
	public void crawl() {

		List<Status> statusList = weiboInterImpl.getPublicStatus();
		if (statusList != null && statusList.size() != 0) {
			// 开启一个存储线程
			new SavePublicStatusThread(statusList).start();
		}
	}
	
	// 存储用户和微博信息
	class SavePublicStatusThread extends Thread {
		
		List<Status> statusList;
		List<StatusBean> statusBeanList = new ArrayList<StatusBean>();
		List<UserBean> userBeanList = new ArrayList<UserBean>();
		
		public SavePublicStatusThread(List<Status> statusList) {
			
			this.statusList = statusList;
		}
		
		public void run() {
			
			// 使用Set去重
			Set<String> statusIdSet = new HashSet<String>();
			Set<String> userIdSet = new HashSet<String>();
			
			for (Status status : statusList) {
				if (statusIdSet.add(status.getId())) {
					statusBeanList.add(StatusBean.getStatusBean(status));
				}
				if (userIdSet.add(status.getUser().getId())) {
					userBeanList.add(UserBean.getUserBean(status.getUser()));
				}
			}
			logger.info("此次共获取公共微博" + statusBeanList.size() + "条，用户" + userBeanList.size() + "人");
			
			// 存储
			DBOperation.insert2StatusesTable(statusBeanList);
			DBOperation.insert2UsersTable(userBeanList);
		}
	}

}

package bnrc.weibo.crawler.util;

import java.util.List;

import weibo4j.model.Status;

public interface WeiboInter {
	
	// 获取最新的公共微博
	public List<Status> getPublicStatus();
	
}

package com.jincou.rocketmq.controller;


import com.jincou.rocketmq.jms.JmsConfig;
import com.jincou.rocketmq.jms.Producer;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
public class Controller {

	@Autowired
	private Producer producer;

	private List<String> mesList;

	public Controller() {
		mesList = new ArrayList<>();
		mesList.add("小小");
		mesList.add("爸爸");
		mesList.add("妈妈");
		mesList.add("爷爷");
		mesList.add("奶奶");
	}

	@RequestMapping("/text/rocketmq")
	public Object callback() throws Exception {
		// 总共发送五次消息
		for (String s : mesList) {
			// 创建生产信息
			Message message = new Message(JmsConfig.TOPIC, "testtag", ("小小一家人的称谓:" + s).getBytes());
			// 发送
			SendResult sendResult = producer.getProducer().send(message);
			log.info("输出生产者信息={}", sendResult);
		}
		return "成功";
	}

	@GetMapping("/message")
	public void message() throws Exception {
		// 同步
		sync();
		// 异步
		async();
		// 单项发送
		oneWay();
	}

	/**
	 * 1、同步发送消息
	 */
	private void sync() throws Exception {
		// 创建消息
		Message message = new Message("topic_family", ("  同步发送  ").getBytes());
		// 同步发送消息
		SendResult sendResult = producer.getProducer().send(message);
		log.info("Product-同步发送-Product信息={}", sendResult);
	}

	/**
	 * 2、异步发送消息
	 */
	private void async() throws Exception {
		// 创建消息
		Message message = new Message("topic_family", ("  异步发送  ").getBytes());
		// 异步发送消息
		producer.getProducer().send(message, new SendCallback() {
			@Override
			public void onSuccess(SendResult sendResult) {
				log.info("Product-异步发送-输出信息={}", sendResult);
			}
			@Override
			public void onException(Throwable e) {
				log.error("出现错误", e);
				// 补偿机制，根据业务情况进行使用，看是否进行重试
			}
		});
	}

	/**
	 * 3、单项发送消息
	 */
	private void oneWay() throws Exception {
		// 创建消息
		Message message = new Message("topic_family", (" 单项发送 ").getBytes());
		// 同步发送消息
		producer.getProducer().sendOneway(message);
	}
}

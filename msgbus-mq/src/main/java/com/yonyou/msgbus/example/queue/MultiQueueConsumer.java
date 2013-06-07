package com.yonyou.msgbus.example.queue;

public class MultiQueueConsumer extends Thread {

	@Override
	public void run() {
		// TODO Auto-generated method stub
		QueueConsumer c = null;

		c = new QueueConsumer();

		String[] myArgs = new String[3];
		myArgs[0] = "test";
		myArgs[1] = "0";
		myArgs[2] = "5000";

		try {
			c.recv(myArgs);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// System.out.println("receiving finished.");
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		// PropertyConfigurator.configure("F:/Java Projects2/mq-test/log4j.properties");
		MultiQueueConsumer m1 = new MultiQueueConsumer();
		MultiQueueConsumer m2 = new MultiQueueConsumer();

		m1.start();
		m2.start();

	}

}

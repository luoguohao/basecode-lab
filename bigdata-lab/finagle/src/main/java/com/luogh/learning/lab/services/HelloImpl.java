package com.luogh.learning.lab.services;

import com.twitter.util.Future;
/**
 * Created by Kaola on 2016/3/31.
 */
public class HelloImpl implements Hello.ServiceIface {
	public Future<String> helloString(String para) {

		return Future.value(para);
	}

	public Future<Integer> helloInt(int para) {

		return Future.value(para);
	}

	public Future<Boolean> helloBoolean(boolean para) {

		return Future.value(para);
	}

	public Future<Void> helloVoid() {
		return Future.value(null);
	}

	public Future<String> helloNull() {
		return Future.value(null);
	}
}

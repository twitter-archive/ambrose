package com.twitter.ambrose.pig;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.tools.pigstats.PigProgressNotificationListener;

public class AmbroseUtil {
	
	protected static Log log = LogFactory.getLog(AmbroseUtil.class);
	
	/**
	 * Uses dynamic proxy to create an instance of PPNL that ignores all the exceptions
	 * @return new Instance of PPNL that ignores all the exceptions
	 */
	public static PigProgressNotificationListener newPPNLWithoutExceptions(final PigProgressNotificationListener ppnl) {
		return (PigProgressNotificationListener)
				Proxy.newProxyInstance(
						AmbroseUtil.class.getClassLoader(), 
						new Class[] {PigProgressNotificationListener.class}, 
						new InvocationHandler() {

							@Override
							public Object invoke(Object proxy, Method method, Object[] args)
									throws Throwable {
								try {
									return method.invoke(ppnl, args);
								} catch (InvocationTargetException e) {
									log.warn("Exception while calling " + method.getName() + 
											" Message:" + e.getTargetException().getLocalizedMessage() + ". Ignoring...");
									log.debug(e.getTargetException());
								}
								return null;
							}
						});
	}

}

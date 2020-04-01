package com.amazonaws.process;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;

public class GetUtil {

	public static void main(String[] args) throws IOException {
		System.out.println(PiUtil());
	}
	
	//function returns the CPU Utilization of the Pi 
		private static double PiUtil() throws IOException {
			
			 double result = 0.0;
			 OperatingSystemMXBean operatingSystemMXBean = ManagementFactory.getOperatingSystemMXBean();
			  for (Method method : operatingSystemMXBean.getClass().getDeclaredMethods()) {
			    method.setAccessible(true);
			    if (method.getName().startsWith("get") && Modifier.isPublic(method.getModifiers())) {
			            Object value;
			        try {
			            value = method.invoke(operatingSystemMXBean);
			        } catch (Exception e) {
			            value = e;
			        } // try
			        if(method.getName().toString().equals("getSystemCpuLoad")) {
			        	System.out.println(method.getName() + " = " + value);
			        	result = (double) value;
			        }
			       // System.out.println(method.getName() + " = " + value);
			    } // if
			  } // for
			 return result * 100 ;
			
		}
}

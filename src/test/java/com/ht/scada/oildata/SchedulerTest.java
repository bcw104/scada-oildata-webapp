package com.ht.scada.oildata;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.testng.annotations.Test;
import org.testng.annotations.BeforeTest;

@ContextConfiguration(locations={"classpath:/META-INF/applicationContext.xml"})
public class SchedulerTest {
  @BeforeTest
  public void beforeTest() {
//	  ApplicationContext context = new ClassPathXmlApplicationContext("applicationContext.xml"); 
  }


  @Test
  public void init() {
    new Scheduler().init();
  }
}

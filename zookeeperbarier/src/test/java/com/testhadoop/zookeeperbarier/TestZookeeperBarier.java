package com.testhadoop.zookeeperbarier;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;
import org.junit.Before;
import org.junit.Test;


public class TestZookeeperBarier {
	private final String address = "10.32.12.34";
	private final String nodeRoot1 = "/barier1";
	private final String nodeRoot2 = "/barier2";
	private Barier barier1;
	private Barier barier2;

	@Before
	public void setUp() throws KeeperException, InterruptedException, IOException
	{
		barier1 = new Barier(address,nodeRoot1,1,"node1");
		barier2 = new Barier(address,nodeRoot2,1, null);
	}	
	@Test
	public void testBarier() throws KeeperException, InterruptedException, IOException {
		System.out.println("Entering barier");		
		barier1.enter();	
		System.out.println("Entered barier");
		System.in.read();
		System.out.println("Leaving barier");
		barier1.leave();
		System.out.println("Leaved barier");
		System.in.read();	
		System.out.println("Entering barier");		
		barier2.enter();	
		System.out.println("Entered barier");
		System.in.read();
		System.out.println("Leaving barier");
		barier2.leave();
		System.out.println("Leaved barier");	
	}
}

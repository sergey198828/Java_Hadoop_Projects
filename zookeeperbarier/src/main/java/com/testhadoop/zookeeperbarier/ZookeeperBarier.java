package com.testhadoop.zookeeperbarier;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;

public class ZookeeperBarier {
	
	public static void main(String[] args) throws KeeperException, InterruptedException, IOException {
		String address;
		String name;
		int size;
		try{
			address = args[0];
			name = args[1];
			size = Integer.parseInt(args[2]);
		}catch(Exception e){
			System.out.println("Arguments are not specified properly");
			System.out.println(e.toString());
			return;
		}
		Barier barier = new Barier(address, name, size, null);
		System.out.println("Entering barier");
		barier.enter();
		System.out.println("Entered barier, press Enter to leave");
		System.in.read();
		System.out.println("Leaving barier");
		barier.leave();
		System.out.println("Leaved barier, bye!");		
	}

}

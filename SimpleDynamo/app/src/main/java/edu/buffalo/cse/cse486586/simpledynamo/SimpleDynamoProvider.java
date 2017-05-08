package edu.buffalo.cse.cse486586.simpledynamo;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.net.Uri;
import android.telephony.TelephonyManager;
import android.util.Log;

import static android.content.ContentValues.TAG;

public class SimpleDynamoProvider extends ContentProvider {


	public static class Node{

		String AvdNo;
		String hashavd;
		Node SNode;
		Node PNode;

		public Node(String AvdNo, String hashavd, Node SNode, Node PNode) {
			this.AvdNo = AvdNo;
			this.hashavd = hashavd;
			this.SNode = SNode;
			this.PNode = PNode;
		}

		public String getAvdNo() {
			return AvdNo;
		}

		public void setAvdNo(String AvdNo) {
			this.AvdNo = AvdNo;
		}

		public String getHashavd() {
			return hashavd;
		}

		public void setHashavd(String hashavd) {
			this.hashavd = hashavd;
		}

		public Node getSNode() {
			return SNode;
		}

		public void setSNode(Node SNode) {
			this.SNode = SNode;
		}

		public Node getPNode() {
			return PNode;
		}

		public void setPNode(Node PNode) {
			this.PNode = PNode;
		}
	}


	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}



	//------------------------------------------------------------------------------------------------------ global declarations

	private final Uri mNewUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo");

	static final String[] REMOTE_PORT = {"11108", "11112", "11116", "11120", "11124"};
	static final int SERVER_PORT = 10000;
	String node_id="";
	ArrayList<Node> NodeList = new ArrayList();
	Node node;
	String predNode=null;
	String succNode=null;
	BlockingQueue<String> q = new ArrayBlockingQueue<String>(1);

	//------------------------------------------------------------------------------------------------------



	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean onCreate() {

		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));

		if(!myPort.equals(REMOTE_PORT[0])){
			try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

		}

		Log.d(TAG,"Entered OnCreate: "+myPort);

		try {
			node_id = genHash(portStr);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}


		if(portStr.equals("5554")){

			node = new Node(portStr, node_id, null, null); // ----------------------------------------------- creating base node for each AVD
			NodeList.add(node);
		}

		else if(portStr.equals("5556")){

			node = new Node(portStr, node_id, null, null); // ----------------------------------------------- creating base node for each AVD
			NodeList.add(node);
		}

		else if(portStr.equals("5558")){

			node = new Node(portStr, node_id, null, null); // ----------------------------------------------- creating base node for each AVD
			NodeList.add(node);
		}

		else if(portStr.equals("5560")){

			node = new Node(portStr, node_id, null, null); // ----------------------------------------------- creating base node for each AVD
			NodeList.add(node);
		}

		else if(portStr.equals("5562")){

			node = new Node(portStr, node_id, null, null); // ----------------------------------------------- creating base node for each AVD
			NodeList.add(node);
		}



		return false;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
}

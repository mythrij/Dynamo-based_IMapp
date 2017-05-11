package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
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
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import static android.content.ContentValues.TAG;

public class SimpleDynamoProvider extends ContentProvider {


	public static class Node {

		String AvdNo;
		String hashavd;
		String SNode;
		String PNode;

		public Node(String AvdNo, String hashavd, String SNode, String PNode) {
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

		public String getSNode() {
			return SNode;
		}

		public void setSNode(String SNode) {
			this.SNode = SNode;
		}

		public String getPNode() {
			return PNode;
		}

		public void setPNode(String PNode) {
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
	String node_id = "";
	ArrayList<Node> NodeList = new ArrayList();
	Node node;

	//------------------------------------------------------------------------------------------------------

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		File[] filearray = getContext().getFilesDir().listFiles();

		if (!selection.equals("@") && !selection.equals("*")) {
			for (File f : filearray) {
				if (f.getName().equals(selection)) {
					f.delete();
				}
			}
		}

		else if (selection.equals("@")) {
			for (File f : filearray) {
				f.delete();
			}
		}

		else if (selection.equals("*")) {
			// call client task
			for (File f : filearray) {
				f.delete();
			}
			String del = "delete"+":"+"xyz"+"\n";
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, del);
		}

		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	public String genList(String hashed_avd) {

		String list_nodes = "";
		for (Node n:NodeList) {
			try {
				if ((hashed_avd.compareTo(n.hashavd) < 0 && hashed_avd.compareTo(genHash(n.PNode)) > 0)
						|| (genHash(n.PNode).compareTo(n.hashavd) > 0
						&& (hashed_avd.compareTo(genHash(n.PNode)) > 0 || hashed_avd.compareTo(n.hashavd) < 0))) {

					list_nodes = n.AvdNo;

					list_nodes = list_nodes + ":" + n.SNode + ":";

					String temp = "";
					for (Node n1 : NodeList) {
						if (n1.getAvdNo().equals(n.SNode)) {
							temp = n1.SNode;
						}
					}

					list_nodes += temp;
					return list_nodes;
				}
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
		}
		return list_nodes;
	}


	@Override
	public Uri insert(Uri uri, ContentValues values) {

		String filename = values.getAsString("key");
		String string = values.getAsString("value");
		FileOutputStream outputStream;


		if (string.contains("#")) {
			try {
				Log.d(TAG, "Inserting into current node, no conditions to be checked");
				Context ctx = getContext();
				if (ctx != null) {
					outputStream = ctx.openFileOutput(filename, Context.MODE_PRIVATE);
					outputStream.write(string.getBytes());
					outputStream.close();
				}
			} catch (Exception e) {
				Log.e(TAG, "File write failed");
			}
		} else if (!string.contains("#")) {

			string = string + "#" + System.currentTimeMillis();

			//hashing the key and comparing

			try {

				String keyhash = genHash(filename);

				String list = genList(keyhash);
				Log.d(TAG,"Nodes to insert key to: " + list);
				String[] p = list.split(":");
				String rep2 = p[2];
				String rep1 = p[1];
				String coord = p[0];

				String sendToClient = "";

				//call function to find out coordinator and replicas -

				Log.d(TAG, "Insert called for: " + filename + " " + string);

				//if string contains timestamp, call is coming from another server. So directly insert and do not call clienttask.
				// else do below

				if (node.AvdNo.equals(coord) || node.AvdNo.equals(rep1) || node.AvdNo.equals(rep2)) {

					//------------------------------------------------------------------------------------------- writing message to file
					try {
						Log.d(TAG, "Inserting into current node");
						Context ctx = getContext();
						if (ctx != null) {
							outputStream = ctx.openFileOutput(filename, Context.MODE_PRIVATE);
							outputStream.write(string.getBytes());
							outputStream.close();
						}
					} catch (Exception e) {
						Log.e(TAG, "File write failed");
					}

					Log.d(TAG, "Calling clienttask function");


					//if it was inserted into coord
					if (node.AvdNo.equals(coord)) {
						sendToClient = "insert2:" + rep1 + ":" + rep2 + ":" + filename + ":" + string + "\n";
					}
					//if it was inserted into rep1
					if (node.AvdNo.equals(rep1)) {
						sendToClient = "insert2:" + coord + ":" + rep2 + ":" + filename + ":" + string + "\n";
					}
					//if it was inserted into rep2
					if (node.AvdNo.equals(rep2)) {
						sendToClient = "insert2:" + coord + ":" + rep1 + ":" + filename + ":" + string + "\n";
					}

					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, sendToClient);

				} else {

					sendToClient = "insertCRR:" + coord + ":" + rep1 + ":" + rep2 + ":" + filename + ":" + string + "\n";
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, sendToClient);        // if it doesnt belong to any of the three nodes, send all three to client as part of mssg to be sent to servers

				}

			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
		}

		return uri;
	}


	@Override
	public boolean onCreate() {

		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));

		Log.d(TAG, "Entered OnCreate: " + myPort);

		try {
			node_id = genHash(portStr);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}


		if (portStr.equals("5554")) {
			node = new Node(portStr, node_id, "5558", "5556"); // ----------------------------------------------- creating base node for each AVD
		} else if (portStr.equals("5556")) {
			node = new Node(portStr, node_id, "5554", "5562"); // ----------------------------------------------- creating base node for each AVD
		} else if (portStr.equals("5558")) {
			node = new Node(portStr, node_id, "5560", "5554"); // ----------------------------------------------- creating base node for each AVD
		} else if (portStr.equals("5560")) {
			node = new Node(portStr, node_id, "5562", "5558"); // ----------------------------------------------- creating base node for each AVD
		} else if (portStr.equals("5562")) {
			node = new Node(portStr, node_id, "5556", "5560"); // ----------------------------------------------- creating base node for each AVD
		}

		//creating a master nodeList within each avd---------------

		try {

			NodeList.add(new Node("5554", genHash("5554"), "5558", "5556"));
			NodeList.add(new Node("5556", genHash("5556"), "5554", "5562"));
			NodeList.add(new Node("5558", genHash("5558"), "5560", "5554"));
			NodeList.add(new Node("5560", genHash("5560"), "5562", "5558"));
			NodeList.add(new Node("5562", genHash("5562"), "5556", "5560"));

		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}

		try {
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		} catch (IOException e) {
			Log.e(TAG, "Can't create a ServerSocket");
			return false;
		}

		return false;
	}

	public String splitcompare(String val1, String val2) {
		Log.d(TAG,"Comparing: " + val1 + " || " + val2);
		String[] p1 = val1.split("#");
		Log.d(TAG,"p1 is: " + p1[0]);
		String ts1 = p1[1];
		String[] p2 = val2.split("#");
		String ts2 = p2[1];

		if (ts1.compareTo(ts2) > 0) {
			return val1;
		} else
			return val2;

	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
						String sortOrder) {

		Context ctx = getContext();
		MatrixCursor c = null;

		if (!selection.equals("@") && !selection.equals("*")) {

			String SelectionHash = null;

			try {

				SelectionHash = genHash(selection);

				if (selectionArgs == null) {

					String list = genList(SelectionHash);
					String[] p = list.split(":");
					String rep2 = p[2];
					String rep1 = p[1];
					String coord = p[0];
					String kv = "";
					String senderport = node.AvdNo;
					String query2 = "";

					try {

						if (node.AvdNo.equals(coord) || node.AvdNo.equals(rep1) || node.AvdNo.equals(rep2)) {

							Log.d("QUERY", "QUERY exists here. Retreiving...");
							FileInputStream fis = null;

							fis = ctx.openFileInput(selection);
							InputStreamReader isr = new InputStreamReader(fis);
							BufferedReader br = new BufferedReader(isr);
							kv = br.readLine();

							Log.v("file content: ", kv);

							if (node.AvdNo.equals(coord)) {
								query2 = "query2:" + rep1 + ":" + rep2 + ":" + senderport + ":" + selection;
							} else if (node.AvdNo.equals(rep1)) {
								query2 = "query2:" + coord + ":" + rep2 + ":" + senderport + ":" + selection;
							} else if (node.AvdNo.equals(rep2)) {
								query2 = "query2:" + coord + ":" + rep1 + ":" + senderport + ":" + selection;
							}
						}

						else {

							query2 = "query2:" + coord + ":" + rep1 + ":" + rep2 + ":" + senderport + ":" + selection;

						}

						BlockingQueue<String> q = new ArrayBlockingQueue<String>(1);
						new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, query2, q); //sending to client to fetch from other 2/3 nodes
						String valueOtherServers = q.take();
						// compare kv and valueOtherServers

						if (!kv.equals(""))
							kv = splitcompare(kv, valueOtherServers);
						else
							kv = valueOtherServers;

						c = new MatrixCursor(new String[]{"key", "value"});
						c.addRow(new Object[]{selection, kv.split("#")[0]});


					} catch (FileNotFoundException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					}

				}



			else if (selectionArgs != null && selectionArgs[0].equals("RP")) {

				Log.d("QUERY", "Remote call. Sending back original port");
				try {

					FileInputStream fis = ctx.openFileInput(selection);
					InputStreamReader isr = new InputStreamReader(fis);
					BufferedReader br = new BufferedReader(isr);
					String kv;
					kv = br.readLine();
					//String q2 = "qback" + ":" + selection + ":" + kv + ":" + selectionArgs[0] + ":\n";

					c = new MatrixCursor(new String[]{"key", "value"});
					c.addRow(new Object[]{selection, kv});

				} catch (FileNotFoundException e) {
					e.printStackTrace();
					return null;
				} catch (IOException e) {
					e.printStackTrace();
				}

			}
			} catch (Exception e) {
				Log.i(TAG, "Exception e: " + e.toString());
				e.printStackTrace();

			}

		}
		else if (selection.equals("@") || selection.equals("*")) {

			c = new MatrixCursor(new String[]{"key", "value"});

			File[] filearray = getContext().getFilesDir().listFiles();

			for (File f : filearray) {

				FileInputStream fis = null;
				try {
					fis = ctx.openFileInput(f.getName());
					InputStreamReader isr = new InputStreamReader(fis);
					BufferedReader br = new BufferedReader(isr);
					String kv;
					kv = br.readLine();
					Log.v("file content: ", kv);
					kv = kv.split("#")[0];
					c.addRow(new Object[]{f.getName(), kv});

                    /*if (selectionArgs != null) {
                        Log.d("QUERY", "Remote call. Sending to original port");
                        String q2 = "query2" + ":" + selection + ":" + kv + ":" + selectionArgs[0] + ":\n";

                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, q2);

                    }*/


				} catch (FileNotFoundException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}

			}

			if (selection.equals("*")) {
				String starreq = "StarQuery" + ":" + node.getAvdNo() + "\n";
				BlockingQueue<String> q = new ArrayBlockingQueue<String>(1);
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, starreq,q);

				String keyVals="";
				try {
					keyVals = q.take();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

				Log.d(TAG,"Star key vals Retrieved: "+keyVals);
				if(!keyVals.equals("")){
					String[] pair = keyVals.split("@");

					for(String kv:pair){
						Log.d(TAG,"Star KV Pair: "+kv);

						if(!(kv.equals("") || kv==null )) {
							String[] kvpair = kv.split(":");
							c.addRow(new Object[]{kvpair[0], kvpair[1]});
						}
					}
				}


			}
		}

		return c;

	}


	@Override
	public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
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


	//------------------------------------------------------------------------------------------------------------ SERVER TASK ----------------

	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		@Override
		protected Void doInBackground(ServerSocket... sockets) {

			Socket socket;
			ServerSocket serverSocket = sockets[0];

			try {

				// taken frokm PA1
           /*Java socket API tutorials and references on the internet used for standard syntax pertaining to
           *reading and writing from and to sockets.
           */

				final String KEY_FIELD = "key";
				final String VALUE_FIELD = "value";

				while (true) { //infinite loop for listening

					String message = "";

					socket = serverSocket.accept();

					InputStream ipstream = socket.getInputStream();
					InputStreamReader ipsreader = new InputStreamReader(ipstream);
					BufferedReader br = new BufferedReader(ipsreader);
					message = br.readLine();
					Log.d(TAG, "msg in " + message);

					//-------------------------------------------------------------------------------------------------------------- received port-num and hashed value

					String[] parts = message.split(":");
					String flag = parts[0];

					if(flag.equals("delete")){

						OutputStream o2stream = socket.getOutputStream();
						OutputStreamWriter op2writer = new OutputStreamWriter(o2stream);
						BufferedWriter b2 = new BufferedWriter(op2writer);
						delete(mNewUri,"@",null);
						b2.write(node.SNode);
						b2.flush();
						socket.close();

					}

					else if(flag.equals("StarQuery")){

						Cursor cursor = query(mNewUri, null, "@", null, null);
						String out = "StarQueryAck-";
						if(cursor.moveToFirst()){
							do{
								String key = cursor.getString(cursor.getColumnIndex("key"));
								String value = cursor.getString(cursor.getColumnIndex("value"));
								out += key+":"+value+"@";

							}while(cursor.moveToNext());
						}


						out+= "-"+node.SNode;
						OutputStream o2stream = socket.getOutputStream();
						OutputStreamWriter op2writer = new OutputStreamWriter(o2stream);
						Log.d(TAG, "Fetched Values form: "+node.getAvdNo() + ": "+out);
						BufferedWriter b2 = new BufferedWriter(op2writer);
						b2.write(out+"\n");
						b2.flush();
						socket.close();

					}

					else if(flag.equals("other2queries")) {
						//other2queries:key:senderport

						String key = parts[1];
						String senderP = parts[2];
						String[] temp = {"RP",senderP};

						Cursor cursor = query(mNewUri, null, key, temp, null);
						String out = "";
						Log.d(TAG,"Received cursor");
						if(cursor.moveToFirst()){
							do{
								String k = cursor.getString(cursor.getColumnIndex("key"));
								String value = cursor.getString(cursor.getColumnIndex("value"));
								out = value;

							}while(cursor.moveToNext());
						}

						Log.d(TAG,"Writing query value: " + out);
						OutputStream o1stream = socket.getOutputStream();
						OutputStreamWriter op1writer = new OutputStreamWriter(o1stream);
						BufferedWriter b1 = new BufferedWriter(op1writer);
						b1.write(out); //------------------------------------------------------------------------------------sent back to client of sender
						b1.flush();
						socket.close();

					}


					if (flag.equals("plsInsertDirect")) {

						String key = parts[1];
						String m = parts[2];

						// Defines an object to contain the new values to insert
						ContentValues mNewValues = new ContentValues();
						mNewValues.put(KEY_FIELD, key);
						mNewValues.put(VALUE_FIELD, m);

						insert(mNewUri, mNewValues);
						OutputStream ostream = socket.getOutputStream();
						OutputStreamWriter opwriter = new OutputStreamWriter(ostream);
						BufferedWriter b = new BufferedWriter(opwriter);
						b.write("InsertAckCRR"); //------------------------------------------------------------------------------------sent back to client of sender
						b.flush();
						socket.close();
					}
					else if (flag.equals("plsInsert2")) {

						String key = parts[1];
						String m = parts[2];

						ContentValues mNewValues = new ContentValues();
						mNewValues.put(KEY_FIELD, key);
						mNewValues.put(VALUE_FIELD, m);

						insert(mNewUri, mNewValues);
						OutputStream ostream = socket.getOutputStream();
						OutputStreamWriter opwriter = new OutputStreamWriter(ostream);
						BufferedWriter b = new BufferedWriter(opwriter);
						b.write("InsertAck2"); //------------------------------------------------------------------------------------sent back to client of sender
						b.flush();
						socket.close();

					}

				}

			} catch (IOException ex) {
				System.out.println(ex.toString());
				System.out.println("Could not find file ");
			}

			return null;
		}
	}

	// ------------------------------------------------------------------------------------------------------------ CLIENT TASK --------------


	private class ClientTask extends AsyncTask<Object, Void, Void> {

		@Override
		protected Void doInBackground(Object... msgs) {
			try {

				String key, mssg, f;
				String x = msgs[0].toString();

				Log.d(TAG, "Received msg is: " + msgs[0].toString());

				String[] getparts = x.split(":");
				f = getparts[0];

				if(f.equals("delete")){
					Log.d(TAG,"Calling delete");

					String nextdel = node.SNode;

					while(!node.AvdNo.equals(nextdel)) {
						Log.d(TAG,"Sending delete request to node: " + nextdel);
						Socket sockets4 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(Integer.toString(Integer.parseInt(nextdel) * 2)));

						OutputStream stream = sockets4.getOutputStream();                           //-------------------sending to server node ot call delete for next node
						OutputStreamWriter owriter = new OutputStreamWriter(stream);
						BufferedWriter bw = new BufferedWriter(owriter);
						String del = "delete" + ":" + "xyz" + "\n";
						bw.write(del);
						bw.flush();

						InputStream ipstream = sockets4.getInputStream();
						InputStreamReader ipsreader = new InputStreamReader(ipstream);
						BufferedReader br = new BufferedReader(ipsreader);
						nextdel = br.readLine();
						if (nextdel == null)
							throw new NullPointerException();
						Log.d(TAG,"Delete ack received");
						//if (ack.startsWith("deleted"))
						sockets4.close();
					}

				}

				else if(f.equals("StarQuery")){
					Log.d(TAG,"In query *");
					String avdNo = getparts[1];
					String originalSender = avdNo.trim();
					String sendingTo = node.SNode;

					String allKeyValPairs = "";
					while(!sendingTo.equals(originalSender)) {
						Log.d(TAG,"Sending to: " + sendingTo);
						Socket sockets3 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(Integer.toString(Integer.parseInt(sendingTo) * 2)));

						//String displayThis = "display" + ":" + key + ":" + value + "\n";

						String starmsg = "StarQuery:zzz\n";
						OutputStream stream = sockets3.getOutputStream();                           //-------------------querying being passed onto SuccNode servertask
						OutputStreamWriter owriter = new OutputStreamWriter(stream);
						BufferedWriter bw = new BufferedWriter(owriter);
						bw.write(starmsg);

						bw.flush();

						InputStream ipstream = sockets3.getInputStream();
						InputStreamReader ipsreader = new InputStreamReader(ipstream);
						BufferedReader br = new BufferedReader(ipsreader);
						String ack = br.readLine();
						if (ack == null) {
							sockets3.close();
							throw new NullPointerException();
						}
						if (ack.startsWith("StarQueryAck")) {
							String[] inf = ack.split("-");
							Log.d(TAG, "Received Star key vals from: "+sendingTo +" next node: "+inf[2].trim());
							sendingTo = inf[2].trim();

							Log.d(TAG,"Star Original Sender: "+originalSender +" Sending TO: "+sendingTo);

							allKeyValPairs+= inf[1]+"@";

							sockets3.close();
						}


					}

					try {
						ArrayBlockingQueue<String> q = (ArrayBlockingQueue<String>) msgs[1];
						q.put(allKeyValPairs);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}

				}

				if (f.equals("query2")) {
					// query2:node1:node2:originalPort:key
					int n = getparts.length;

				//	if(getparts.length == 6){
					String senderport = getparts[n-2];
					key = getparts[n-1];
					n = n-2;
					String get2query = "other2queries:"+ key + ":" + senderport+"\n";

					String finalResult = null;
					for (int i = 1; i<n; i++) {
						Log.d(TAG,"Sending Query for key: " + key + " to avd: " + getparts[i]);
						Socket sockets2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(Integer.toString(Integer.parseInt(getparts[i]) * 2)));

						OutputStream stream = sockets2.getOutputStream();                           //-------------------query request being passed onto other 2 nodes
						OutputStreamWriter owriter = new OutputStreamWriter(stream);
						BufferedWriter bw = new BufferedWriter(owriter);
						bw.write(get2query);
						bw.flush();

						InputStream ipstream = sockets2.getInputStream();
						InputStreamReader ipsreader = new InputStreamReader(ipstream);
						BufferedReader br = new BufferedReader(ipsreader);
						String ack = br.readLine();
						Log.d(TAG,"other2queries ack: " + ack);
						if (ack == null) {
							sockets2.close();
							throw new NullPointerException();
						}
						else {
							sockets2.close();
							if (finalResult == null)
								finalResult = ack;
							else {
								// compare new ack with existing finalResult and replace if necessary
								finalResult=splitcompare(ack,finalResult);
							}

						}

					}

					ArrayBlockingQueue<String> q = (ArrayBlockingQueue<String>) msgs[1];
					q.put(finalResult);


				}//end of query2

				if (f.equals("insertCRR")) {

					key = getparts[4];
					mssg = getparts[5];

					for (int a = 1; a <= 3; a++) {
						Log.d(TAG,"Sending to: " + getparts[a]);
						Socket sockets = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(Integer.toString(Integer.parseInt(getparts[a]) * 2)));


						String msgToInsert = "plsInsertDirect" + ":" + key + ":" + mssg + "\n";

						OutputStream stream = sockets.getOutputStream();
						OutputStreamWriter owriter = new OutputStreamWriter(stream);                    //-------------------sending message and key to each of the C-R-R to insert directly
						owriter.write(msgToInsert);
						owriter.flush();

						//reading socket for the acknowledgment
						InputStream ipstream = sockets.getInputStream();
						InputStreamReader ipsreader = new InputStreamReader(ipstream);
						BufferedReader br = new BufferedReader(ipsreader);
						String ack = br.readLine();
						Log.d(TAG,"plsInsertDirect ack: " + ack);
						if (ack == null) {
							sockets.close();
							throw new NullPointerException();
						}
						if (ack.startsWith("InsertAckCRR")) {
							sockets.close();
							Log.i(TAG,"Ack received: " + ack);
						}
					}

				} else if (f.equals("insert2")) {

					key = getparts[3];
					mssg = getparts[4];

					for (int a = 1; a <= 2; a++) {

						Socket sockets = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(Integer.toString(Integer.parseInt(getparts[a]) * 2)));


						String msgToInsert = "plsInsert2" + ":" + key + ":" + mssg + "\n";

						OutputStream stream = sockets.getOutputStream();
						OutputStreamWriter owriter = new OutputStreamWriter(stream);                    //-------------------sending message and key to each of the C-R-R to insert directly
						owriter.write(msgToInsert);
						owriter.flush();

						//reading socket for the acknowledgment
						InputStream ipstream = sockets.getInputStream();
						InputStreamReader ipsreader = new InputStreamReader(ipstream);
						BufferedReader br = new BufferedReader(ipsreader);
						String ack = br.readLine();
						Log.d(TAG,"plsInsert2 ack: " + ack);
						if (ack == null) {
							sockets.close();
							throw new NullPointerException();
						}
						else if (ack.startsWith("InsertAck2"))
							sockets.close();
					}
				}

			}
			catch(Exception e){
				Log.e(TAG, "ClientTask Exception: " + e.toString());
				e.printStackTrace();
			}

			return null;
		}
	}
}


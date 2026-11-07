package utb.fai;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

public class SocketHandler {
	/** mySocket je socket, o který se bude tento SocketHandler starat */
	Socket mySocket;

	/** client ID je řetězec ve formátu <IP_adresa>:<port> */
	String clientID;

	/** Uživatelské jméno klienta */
	private String userName = null;

	/** Množina diskuzních místností, ve kterých se klient nachází */
	private final Set<String> groups = new CopyOnWriteArraySet<>();

	/**
	 * activeHandlers je reference na množinu všech právě běžících SocketHandlerů.
	 */
	ActiveHandlers activeHandlers;

	/**
	 * messages je fronta příchozích zpráv pro tohoto klienta.
	 */
	ArrayBlockingQueue<String> messages = new ArrayBlockingQueue<String>(100);

	/**
	 * startSignal je synchronizační závora pro spuštění obou tasků současně.
	 */
	CountDownLatch startSignal = new CountDownLatch(2);

	/** outputHandler se stará o OutputStream socketu */
	OutputHandler outputHandler = new OutputHandler();
	/** inputHandler se stará o InputStream socketu */
	InputHandler inputHandler = new InputHandler();

	volatile boolean inputFinished = false;

	public SocketHandler(Socket mySocket, ActiveHandlers activeHandlers) {
		this.mySocket = mySocket;
		this.clientID = mySocket.getInetAddress().toString() + ":" + mySocket.getPort();
		this.activeHandlers = activeHandlers;
	}

	public String getUserName() {
		return userName;
	}

	public Set<String> getGroups() {
		return groups;
	}

	class OutputHandler implements Runnable {
		public void run() {
			OutputStreamWriter writer;
			try {
				System.err.println("DBG>Output handler starting for " + clientID);
				startSignal.countDown();
				startSignal.await();
				System.err.println("DBG>Output handler running for " + clientID);
				writer = new OutputStreamWriter(mySocket.getOutputStream(), "UTF-8");
				writer.write("Enter your name:\r\n");
				writer.flush();
				while (!inputFinished) {
					String m = messages.take();
					if (inputFinished) {
						break;
					}
					writer.write(m + "\r\n");
					writer.flush();
					System.err.println("DBG>Message sent to " + clientID + ":" + m + "\n");
				}
			} catch (IOException | InterruptedException e) {
				// socket uzavřen nebo vlákno přerušeno
			} finally {
				try {
					if (!mySocket.isClosed()) {
						mySocket.close();
					}
				} catch (IOException e) {
					// ignorovat
				}
			}
			System.err.println("DBG>Output handler for " + clientID + " has finished.");
		}
	}

	class InputHandler implements Runnable {
		public void run() {
			try {
				System.err.println("DBG>Input handler starting for " + clientID);
				startSignal.countDown();
				startSignal.await();
				System.err.println("DBG>Input handler running for " + clientID);
				activeHandlers.add(SocketHandler.this);
				BufferedReader reader = new BufferedReader(new InputStreamReader(mySocket.getInputStream(), "UTF-8"));
				String request;
				while ((request = reader.readLine()) != null) {
					request = request.trim();
					if (request.isEmpty()) {
						continue;
					}

					// Pokud uživatel ještě nemá nastavené jméno, čeká se na zadání jména
					if (userName == null) {
						String desiredName = request;
						if (request.startsWith("#setMyName ")) {
							desiredName = request.substring("#setMyName ".length()).trim();
						}
						if (!desiredName.isEmpty() && !desiredName.contains(" ")
								&& activeHandlers.isNameAvailable(desiredName, SocketHandler.this)) {
							userName = desiredName;
							groups.add("public");
						}
						continue;
					}

					// Zpracování příkazů začínajících '#'
					if (request.startsWith("#")) {
						handleCommand(request);
					} else {
						// Běžná zpráva - odeslat do sdílených diskuzních místností
						String formatted = "[" + userName + "] >> " + request;
						System.out.println(formatted);
						activeHandlers.sendMessageToSharedGroups(SocketHandler.this, formatted);
					}
				}
			} catch (IOException | InterruptedException e) {
				// odpojení klienta
			} finally {
				inputFinished = true;
				messages.offer(""); // Probudit outputHandler pro ukončení
				activeHandlers.remove(SocketHandler.this);
				try {
					if (!mySocket.isClosed()) {
						mySocket.close();
					}
				} catch (IOException e) {
					// ignorovat
				}
			}
			System.err.println("DBG>Input handler for " + clientID + " has finished.");
		}

		private void handleCommand(String command) {
			if (command.startsWith("#setMyName ")) {
				String newName = command.substring("#setMyName ".length()).trim();
				if (!newName.isEmpty() && !newName.contains(" ")
						&& activeHandlers.isNameAvailable(newName, SocketHandler.this)) {
					userName = newName;
				}
			} else if (command.startsWith("#sendPrivate ")) {
				String[] parts = command.split(" ", 3);
				if (parts.length >= 3) {
					String recipient = parts[1].trim();
					String privateMsg = parts[2];
					String formatted = "[" + userName + "] >> " + privateMsg;
					activeHandlers.sendPrivateMessage(SocketHandler.this, recipient, formatted);
				}
			} else if (command.startsWith("#join ")) {
				String room = command.substring("#join ".length()).trim();
				if (!room.isEmpty() && !room.contains(" ")) {
					groups.add(room);
				}
			} else if (command.startsWith("#leave ")) {
				String room = command.substring("#leave ".length()).trim();
				if (!room.isEmpty()) {
					groups.remove(room);
				}
			} else if (command.equals("#groups")) {
				String groupList = String.join(",", groups);
				messages.offer(groupList);
			}
		}
	}
}

package utb.fai;

import java.util.*;

public class ActiveHandlers {
    private final Set<SocketHandler> activeHandlersSet = new HashSet<>();

    /**
     * Adds a new handler to the set of active handlers.
     *
     * @param handler The handler to add.
     * @return true if added successfully.
     */
    public synchronized boolean add(SocketHandler handler) {
        return activeHandlersSet.add(handler);
    }

    /**
     * Removes a handler from the set of active handlers.
     *
     * @param handler The handler to remove.
     * @return true if removed successfully.
     */
    public synchronized boolean remove(SocketHandler handler) {
        return activeHandlersSet.remove(handler);
    }

    /**
     * Checks if a user name is valid and not already taken by another active client.
     *
     * @param name The desired user name.
     * @param currentHandler The handler requesting the name check.
     * @return true if name is available and valid.
     */
    public synchronized boolean isNameAvailable(String name, SocketHandler currentHandler) {
        if (name == null || name.trim().isEmpty() || name.contains(" ")) {
            return false;
        }
        for (SocketHandler handler : activeHandlersSet) {
            if (handler != currentHandler && name.equals(handler.getUserName())) {
                return false;
            }
        }
        return true;
    }

    /**
     * Sends a message to all active clients that share at least one group with the sender.
     *
     * @param sender The handler sending the message.
     * @param message The message to broadcast.
     */
    public synchronized void sendMessageToSharedGroups(SocketHandler sender, String message) {
        Set<String> senderGroups = sender.getGroups();
        for (SocketHandler handler : activeHandlersSet) {
            if (handler != sender && handler.getUserName() != null) {
                if (sharesGroup(senderGroups, handler.getGroups())) {
                    if (!handler.messages.offer(message)) {
                        System.err.printf("Client %s message queue is full, dropping the message!\n", handler.clientID);
                    }
                }
            }
        }
    }

    /**
     * Sends a private message to a specific user by name.
     *
     * @param sender The handler sending the message.
     * @param recipientName The target recipient user name.
     * @param message The private message content.
     * @return true if recipient was found and message was queued.
     */
    public synchronized boolean sendPrivateMessage(SocketHandler sender, String recipientName, String message) {
        if (recipientName == null) {
            return false;
        }
        for (SocketHandler handler : activeHandlersSet) {
            if (handler != sender && recipientName.equals(handler.getUserName())) {
                if (!handler.messages.offer(message)) {
                    System.err.printf("Client %s message queue is full, dropping the message!\n", handler.clientID);
                }
                return true;
            }
        }
        return false;
    }

    /**
     * Helper method to verify if two sets of groups have any room in common.
     */
    private boolean sharesGroup(Set<String> set1, Set<String> set2) {
        for (String group : set1) {
            if (set2.contains(group)) {
                return true;
            }
        }
        return false;
    }
}

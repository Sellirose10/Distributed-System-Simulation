"""
Distributed System Simulation
Demonstrates:
- Three processes: P1 Client, P2 Server, P3 Database
- Internal, send, and receive events
- Lamport logical clocks
- Message channels and global state
"""

from collections import deque

class Process:
    def __init__(self, process_id, role):
        self.process_id = process_id
        self.role = role
        self.clock = 0
        self.status = "Ready"
        self.messages_sent = 0
        self.messages_received = 0

    def internal_event(self, description):
        self.clock += 1
        print(f"E{next_event()}: {self.process_id} INTERNAL: {description} | Clock={self.clock}")

    def send(self, destination, message, channel):
        self.clock += 1
        channel.append({
            "source": self.process_id,
            "destination": destination.process_id,
            "message": message,
            "timestamp": self.clock
        })
        self.messages_sent += 1
        print(
            f"E{next_event()}: {self.process_id} SEND -> {destination.process_id}: "
            f"{message} | Clock={self.clock}"
        )

    def receive(self, source, channel):
        if not channel:
            raise RuntimeError(f"No message available for {self.process_id}")
        message = channel.popleft()
        self.clock = max(self.clock, message["timestamp"]) + 1
        self.messages_received += 1
        print(
            f"E{next_event()}: {self.process_id} RECEIVE <- {source.process_id}: "
            f"{message['message']} | Received timestamp={message['timestamp']} "
            f"| Clock={self.clock}"
        )
        return message


_event_number = 0

def next_event():
    global _event_number
    _event_number += 1
    return _event_number


def main():
    global _event_number
    _event_number = 0

    p1 = Process("P1", "Client")
    p2 = Process("P2", "Server")
    p3 = Process("P3", "Database")

    channel_12 = deque()
    channel_23 = deque()
    channel_32 = deque()
    channel_21 = deque()

    print("=" * 58)
    print("       DISTRIBUTED SYSTEM SIMULATION")
    print("=" * 58)
    print("\nInitial logical clocks: P1=0, P2=0, P3=0\n")
    print("--- EVENT LOG ---")

    # P1 creates and sends a request to P2
    p1.status = "Creating request"
    p1.internal_event("Create client request")
    p1.status = "Waiting for response"
    p1.send(p2, "Request", channel_12)

    # P2 receives and processes the request, then queries P3
    p2.status = "Receiving request"
    p2.receive(p1, channel_12)
    p2.status = "Processing request"
    p2.internal_event("Process client request")
    p2.send(p3, "Database Query", channel_23)

    # P3 receives query, searches database, and sends result to P2
    p3.status = "Receiving query"
    p3.receive(p2, channel_23)
    p3.status = "Searching database"
    p3.internal_event("Search database")
    p3.send(p2, "Database Result", channel_32)

    # P2 receives result and sends response to P1
    p2.status = "Receiving database result"
    p2.receive(p3, channel_32)
    p2.status = "Sending response"
    p2.send(p1, "Final Response", channel_21)

    # P1 receives final response
    p1.status = "Receiving response"
    p1.receive(p2, channel_21)
    p1.status = "Completed"
    p2.status = "Completed"
    p3.status = "Completed"

    print("\n--- LOCAL PROCESS STATES ---")
    for process in (p1, p2, p3):
        print(
            f"{process.process_id} ({process.role}): Status={process.status}, "
            f"Clock={process.clock}, Sent={process.messages_sent}, "
            f"Received={process.messages_received}"
        )

    print("\n--- COMMUNICATION CHANNEL STATES ---")
    channels = {
        "P1 -> P2": channel_12,
        "P2 -> P3": channel_23,
        "P3 -> P2": channel_32,
        "P2 -> P1": channel_21,
    }
    for name, channel in channels.items():
        print(f"{name}: {list(channel) if channel else 'Empty'}")

    all_channels_empty = all(not channel for channel in channels.values())
    print("\n--- GLOBAL STATE ---")
    print("Global State = Local states of P1, P2, P3 + all channel states")
    print(f"P1: {p1.status}")
    print(f"P2: {p2.status}")
    print(f"P3: {p3.status}")
    print(f"All communication channels empty: {all_channels_empty}")
    print("Global State: CONSISTENT" if all_channels_empty else "Global State: Messages pending")


if __name__ == "__main__":
    main()
# Queue implementation with menu

queue = []

def enqueue():
    element = input("Enter element to enqueue: ")
    queue.append(element)
    print(f"{element} added to queue.")

def dequeue():
    if len(queue) == 0:
        print("Queue is empty! Cannot dequeue.")
    else:
        removed = queue.pop(0)  # Remove element from front
        print(f"{removed} removed from queue.")

def display():
    print("Current queue:", queue)

while True:
    print("\n--- Queue Operations Menu ---")
    print("1. Enqueue")
    print("2. Dequeue")
    print("3. Display Queue")
    print("4. Exit")

    choice = input("Enter your choice: ")

    if choice == '1':
        enqueue()
    elif choice == '2':
        dequeue()
    elif choice == '3':
        display()
    elif choice == '4':
        print("Exiting...")
        break
    else:
        print("Invalid choice! Please enter 1-4.")

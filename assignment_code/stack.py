# Stack implementation with push/pop menu

stack = []

def push():
    element = input("Enter element to push: ")
    stack.append(element)
    print(f"{element} pushed to stack.")

def pop():
    if len(stack) == 0:
        print("Stack is empty! Cannot pop.")
    else:
        removed = stack.pop()
        print(f"{removed} popped from stack.")

def display():
    print("Current stack:", stack)

while True:
    print("\n--- Stack Operations Menu ---")
    print("1. Push")
    print("2. Pop")
    print("3. Display Stack")
    print("4. Exit")

    choice = input("Enter your choice: ")

    if choice == '1':
        push()
    elif choice == '2':
        pop()
    elif choice == '3':
        display()
    elif choice == '4':
        print("Exiting...")
        break
    else:
        print("Invalid choice! Please enter 1-4.")

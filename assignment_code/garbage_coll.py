import gc

# Enable automatic garbage collection
gc.enable()

class MyObject:
    def __init__(self, name):
        self.name = name
        print(f"{self.name} created")

    def __del__(self):
        print(f"{self.name} destroyed")  # Called when object is garbage collected

# Create objects
obj1 = MyObject("Object 1")
obj2 = MyObject("Object 2")

# Delete references
del obj1
print("Deleted obj1 reference")

# Force garbage collection
print("Collecting garbage...")
gc.collect()  # Forces Python to run garbage collection

# Circular reference example
a = {}
b = {'ref_to_a': a}
a['ref_to_b'] = b

print("Created circular references a <-> b")

# Delete references
del a
del b

print("Deleted circular references, collecting garbage...")
gc.collect()

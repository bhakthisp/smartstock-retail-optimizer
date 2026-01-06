from tensorflow.keras.datasets import mnist
from PIL import Image
import os

# Load MNIST
(_, _), (x_test, y_test) = mnist.load_data()

# Folder to save
save_folder = r"c:/Users/bhakt/OneDrive/Desktop/samrt_real/assignment_code/test_images_mnist"
os.makedirs(save_folder, exist_ok=True)

# Pick one example per digit
for digit in range(10):
    idx = next(i for i, y in enumerate(y_test) if y == digit)
    img_array = x_test[idx]  # 28x28 numpy array
    img = Image.fromarray(img_array)
    img = img.convert('L')  # ensure grayscale
    img.save(os.path.join(save_folder, f"mnist_{digit}.png"))

print(f"✅ MNIST-style test images saved in: {save_folder}")

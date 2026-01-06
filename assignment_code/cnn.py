import os
import numpy as np
from PIL import Image
import tensorflow as tf
from tensorflow.keras import layers, models
from tensorflow.keras.datasets import mnist
from tensorflow.keras.models import load_model
# FIXED PATHS
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # samrt_real/
TEST_FOLDER = os.path.join(BASE_DIR, "assignment_code", "test_images_mnist")
SPECIFIC_FILE = os.path.join(TEST_FOLDER, "mnist_0.png")  # or .jpg

print(f"Looking in: {TEST_FOLDER}")
print(f"Target file: {SPECIFIC_FILE}")

# 1. Train/Load Model
MODEL_FILE = "cnn_mnist.h5"
if os.path.exists(MODEL_FILE):
    print("Loading saved model...")
    model = load_model(MODEL_FILE)
else:
    print("Training CNN...")
    (x_train, y_train), (x_test, y_test) = mnist.load_data()
    x_train = x_train.reshape(-1,28,28,1) / 255.0
    x_test  = x_test.reshape(-1,28,28,1) / 255.0

    model = models.Sequential([
        layers.Conv2D(32, (3,3), activation='relu', input_shape=(28,28,1)),
        layers.MaxPooling2D((2,2)),
        layers.Conv2D(64, (3,3), activation='relu'),
        layers.MaxPooling2D((2,2)),
        layers.Flatten(),
        layers.Dense(64, activation='relu'),
        layers.Dense(10, activation='softmax')
    ])
    model.compile(optimizer='adam', loss='sparse_categorical_crossentropy', metrics=['accuracy'])
    model.fit(x_train, y_train, epochs=3, verbose=1)
    model.save(MODEL_FILE)
    print("Model saved!")

# 2. Preprocess images like MNIST
def preprocess_image_mnist_style(path):
    img = Image.open(path).convert('L')

    # Invert if background is white
    if np.mean(img) > 127:
        img = Image.eval(img, lambda x: 255 - x)

    # Resize while keeping aspect ratio
    img.thumbnail((20, 20), Image.Resampling.LANCZOS)

    # Center in 28x28 black background
    new_img = Image.new('L', (28,28), 0)
    new_img.paste(img, ((28 - img.width)//2, (28 - img.height)//2))

    # Convert to numpy array and normalize
    arr = np.array(new_img, dtype=np.float32) / 255.0
    arr = arr.reshape(1,28,28,1)
    
    # Save processed PNG for verification
    processed_path = path.replace('.png', '_processed.png').replace('.jpg', '_processed.png')
    new_img.save(processed_path)
    print(f"✅ PNG saved: {processed_path}")
    return arr

# 3. Process folder
if not os.path.exists(TEST_FOLDER):
    print(f"Folder missing: {TEST_FOLDER}")
    print("Create: samrt_real/assignment_code/test_images/")
    exit()

# List all images
all_images = [f for f in os.listdir(TEST_FOLDER) if f.lower().endswith(('.png', '.jpg', '.jpeg'))]
print(f"Found images: {all_images}")

if not all_images:
    print("No PNG/JPG images found in folder!")
    exit()


for img_file in all_images:
    full_path = os.path.join(TEST_FOLDER, img_file)
    print(f"\nProcessing: {img_file}")
    
    img_array = preprocess_image_mnist_style(full_path)
    prediction = model.predict(img_array, verbose=0)
    digit = np.argmax(prediction)
    confidence = np.max(prediction) * 100
    
    print(f"PREDICTED: {digit} (confidence: {confidence:.1f}%)")

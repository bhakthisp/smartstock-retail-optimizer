import cv2
import mediapipe as mp
from collections import deque
import math

mp_hands = mp.solutions.hands
hands = mp_hands.Hands(
    static_image_mode=False,
    max_num_hands=1,
    min_detection_confidence=0.7,
    min_tracking_confidence=0.7
)

mp_draw = mp.solutions.drawing_utils
cap = cv2.VideoCapture(0)

gesture_buffer = deque(maxlen=7)

def distance(p1, p2):
    return math.hypot(p1.x - p2.x, p1.y - p2.y)

def detect_gesture(landmarks, handedness):
    # Hand side
    hand_label = handedness.classification[0].label  # 'Left' or 'Right'

    # Thumb logic depends on hand side
    if hand_label == "Right":
        thumb = landmarks[4].x < landmarks[3].x
    else:  # Left hand
        thumb = landmarks[4].x > landmarks[3].x

    index = landmarks[8].y < landmarks[6].y
    middle = landmarks[12].y < landmarks[10].y
    ring = landmarks[16].y < landmarks[14].y
    pinky = landmarks[20].y < landmarks[18].y

    # OK gesture
    ok_gesture = distance(landmarks[4], landmarks[8]) < 0.04

    if ok_gesture and middle and ring and pinky:
        return "OK 👌"
    elif index and middle and not ring and not pinky:
        return "Peace ✌️"
    elif thumb and not any([index, middle, ring, pinky]):
        return "Thumbs Up 👍"
    elif not any([thumb, index, middle, ring, pinky]):
        return "Fist ✊"
    elif all([thumb, index, middle, ring, pinky]):
        return "Open Hand ✋"
    else:
        return "Unknown"

def get_stable_gesture(gesture):
    gesture_buffer.append(gesture)
    return max(set(gesture_buffer), key=gesture_buffer.count)

while True:
    success, frame = cap.read()
    if not success:
        break

    frame = cv2.flip(frame, 1)  # Mirror for natural webcam view
    rgb_frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
    result = hands.process(rgb_frame)

    if result.multi_hand_landmarks:
        for hand_landmarks, handedness in zip(
            result.multi_hand_landmarks,
            result.multi_handedness
        ):
            mp_draw.draw_landmarks(
                frame,
                hand_landmarks,
                mp_hands.HAND_CONNECTIONS
            )

            raw_gesture = detect_gesture(hand_landmarks.landmark, handedness)
            stable_gesture = get_stable_gesture(raw_gesture)

            cv2.putText(
                frame,
                f"{handedness.classification[0].label} Hand | {stable_gesture}",
                (20, 50),
                cv2.FONT_HERSHEY_SIMPLEX,
                1,
                (0, 255, 0),
                2
            )

    cv2.imshow("Orientation-Safe Hand Gesture Detection", frame)
    if cv2.waitKey(1) & 0xFF == 27:
        break

cap.release()
cv2.destroyAllWindows()

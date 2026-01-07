import cv2
import mediapipe as mp
import numpy as np
import math

mp_face = mp.solutions.face_mesh
face_mesh = mp_face.FaceMesh(
    static_image_mode=False,
    max_num_faces=1,
    refine_landmarks=True,
    min_detection_confidence=0.7,
    min_tracking_confidence=0.7
)

mp_draw = mp.solutions.drawing_utils
cap = cv2.VideoCapture(0)

def dist(p1, p2):
    return math.hypot(p1.x - p2.x, p1.y - p2.y)

# Eye landmarks (left eye)
LEFT_EYE = [33, 160, 158, 133, 153, 144]
RIGHT_EYE = [362, 385, 387, 263, 373, 380]

# Eyebrow landmarks
LEFT_BROW = [70, 63]
LEFT_EYE_TOP = 159

# Mouth landmarks
MOUTH_LEFT = 61
MOUTH_RIGHT = 291
MOUTH_TOP = 13
MOUTH_BOTTOM = 14

def eye_aspect_ratio(landmarks, eye):
    A = dist(landmarks[eye[1]], landmarks[eye[5]])
    B = dist(landmarks[eye[2]], landmarks[eye[4]])
    C = dist(landmarks[eye[0]], landmarks[eye[3]])
    return (A + B) / (2.0 * C)

while True:
    ret, frame = cap.read()
    if not ret:
        break

    frame = cv2.flip(frame, 1)
    rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
    results = face_mesh.process(rgb)

    expression = "Neutral"

    if results.multi_face_landmarks:
        for face in results.multi_face_landmarks:
            lm = face.landmark

            # Eye detection
            left_ear = eye_aspect_ratio(lm, LEFT_EYE)
            right_ear = eye_aspect_ratio(lm, RIGHT_EYE)

            eyes_closed = left_ear < 0.21 and right_ear < 0.21

            # Eyebrow raise detection
            brow_dist = dist(lm[LEFT_BROW[0]], lm[LEFT_EYE_TOP])
            eye_height = dist(lm[LEFT_EYE[1]], lm[LEFT_EYE[5]])
            brow_up = brow_dist > eye_height * 0.7

            # Mouth detection
            mouth_width = dist(lm[MOUTH_LEFT], lm[MOUTH_RIGHT])
            mouth_height = dist(lm[MOUTH_TOP], lm[MOUTH_BOTTOM])

            smile_ratio = mouth_height / mouth_width

            if eyes_closed:
                expression = "Eyes Closed 😴"
            elif brow_up:
                expression = "Eyebrow Raised 🤨"
            elif smile_ratio > 0.35:
                expression = "Happy 😊"
            elif smile_ratio < 0.18:
                expression = "Sad ☹️"
            else:
                expression = "Neutral 😐"

            mp_draw.draw_landmarks(
                frame,
                face,
                mp_face.FACEMESH_TESSELATION,
                mp_draw.DrawingSpec(color=(0,255,0), thickness=1, circle_radius=1),
                mp_draw.DrawingSpec(color=(0,0,255), thickness=1)
            )

    cv2.putText(
        frame,
        f"Expression: {expression}",
        (30, 50),
        cv2.FONT_HERSHEY_SIMPLEX,
        1,
        (0, 255, 0),
        2
    )

    cv2.imshow("Face Landmarks & Expression Detection", frame)
    if cv2.waitKey(1) & 0xFF == 27:
        break

cap.release()
cv2.destroyAllWindows()

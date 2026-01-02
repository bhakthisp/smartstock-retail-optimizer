import os
import google.generativeai as genai # pyright: ignore[reportMissingImports]
from dotenv import load_dotenv

def main():
    load_dotenv()
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        raise RuntimeError("Please set GEMINI_API_KEY in .env")

    genai.configure(api_key=api_key)
    model = genai.GenerativeModel("gemini-2.5-flash")  # updated model name
    chat = model.start_chat(history=[])

    print("Gemini Chatbot — type 'exit' or 'quit' to stop.")
    while True:
        user_input = input("You: ").strip()
        if user_input.lower() in ("exit", "quit"):
            print("Bot: Bye!")
            break
        resp = chat.send_message(user_input)
        print("Bot:", resp.text)

if __name__ == "__main__":
    main()

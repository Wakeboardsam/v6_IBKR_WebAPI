import os
import time

def main():
    greeting = os.getenv("BOOT_GREETING", "Default Greeting")
    print("---------------------------------")
    print(f"Bot successfully started!")
    print(f"User greeting from HA config: {greeting}")
    print("---------------------------------")
    
    # Keep the container alive with a heartbeat
    while True:
        time.sleep(60)
        print("Bot is humming along...")

if __name__ == "__main__":
    main()

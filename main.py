import subprocess
import sys

def run_script(script_name):
    print(f"\n----------------------------------------")
    print(f" RUNNING: {script_name}")
    print(f"----------------------------------------\n")

    result = subprocess.run([sys.executable, script_name])

    if result.returncode != 0:
        print(f"Error running {script_name}")
        sys.exit(1)
    else:
        print(f"Completed: {script_name}")

def main():
    print("SmartStock Inventory Optimization Pipeline Started")

    # Step 1 – Insert dataset into MySQL
    run_script("insert_data.py")

    # Step 2 – Load from MySQL & preprocess
    run_script("loadclean.py")

    print("\nALL STEPS COMPLETED SUCCESSFULLY!")
    print("Cleaned dataset is ready: cleaned_retail_data.csv")

if __name__ == "__main__":
    main()


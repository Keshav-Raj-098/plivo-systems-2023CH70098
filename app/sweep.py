import subprocess
import json

profiles = ["profiles/A.json", "profiles/B.json"]
delays = [60, 55, 50, 45, 40, 35, 30]

print(f"{'Profile':<15} | {'Delay':<6} | {'Misses':<20} | {'Overhead':<20} | {'Result':<10}")
print("-" * 80)

for p in profiles:
    for d in delays:
        res = subprocess.run(["python3", "run.py", "--profile", p, "--delay_ms", str(d)], capture_output=True, text=True)
        out = res.stdout
        miss = "N/A"
        ovh = "N/A"
        res_text = "N/A"
        for line in out.splitlines():
            if "deadline misses" in line:
                miss = line.strip()
            elif "bandwidth overhead" in line:
                ovh = line.strip()
            elif "RESULT" in line:
                res_text = line.strip()
        print(f"{p:<15} | {d:<6} | {miss:<20} | {ovh:<20} | {res_text:<10}")


import sys
import subprocess

args = sys.argv[1:]
if len(sys.argv) != 3:
   print("Usage: bridge-submit <cluster> <s3 directory>", file=sys.stderr)
   sys.exit(-1)
   
spark_submit_str= f"spark-submit --py-files bridge.zip --master {args[0]}  --deploy-mode client bridge.py {args[1]}"
process=subprocess.Popen(spark_submit_str,stdout=subprocess.PIPE,stderr=subprocess.PIPE, universal_newlines=True, shell=True)

stdout,stderr = process.communicate()

if process.returncode !=0:
   print(stderr)
print(stdout)

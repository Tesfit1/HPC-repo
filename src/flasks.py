import subprocess
import os

python_path = "C:\\Program Files\\Python311\\python.exe"
# python_path = os.getenv("PYTHON_SCRIPTS_PATH")
# script = ["AuthT.py", "CreateCasebookT.py","Event_DM_T.py","COMMON_Ev_Date.py","Event_IC_T.py","VisitTwoDate.py","VitalSignScreening.py","Inc_Ex.py","Eligibility.py","Eventgroups.py","VisitOneDate.py","SamplingTimePoints.py","DrugAdmin.py","SubstanceUse.py","VitalSignTreatment.py","SetEventDateEndOfStudy.py","EndOfTreatment.py"]
script = ["AuthT.py", "CreateCasebookT.py","Event_IC_T.py","Eventgroups.py","EvtGroupDate.py" ,"Event_DM_T.py","VitalSignScreening.py","SubstanceUse.py"]


for script in script:
    try:
        subprocess.run([python_path, script], check=True)
        print(f"{script} executed successfully:")
    except subprocess.CalledProcessError as e:
        print(f"Error executing {script}: {e.stderr}")
# import subprocess
# import os

# python_path = "C:\\Users\\tesfi\\Documents\\python\\HPC-repo\\.venv\\Scripts\\python.exe"
# # python_path = os.getenv("PYTHON_SCRIPTS_PATH")
# # script = ["AuthT.py", "CreateCasebookT.py","Event_DM_T.py","COMMON_Ev_Date.py","Event_IC_T.py","VisitTwoDate.py","VitalSignScreening.py","Inc_Ex.py","Eligibility.py","Eventgroups.py","VisitOneDate.py","SamplingTimePoints.py","DrugAdmin.py","SubstanceUse.py","VitalSignTreatment.py","SetEventDateEndOfStudy.py","EndOfTreatment.py"]
# script = ["AuthT.py", "CreateCasebookT.py","Event_IC_T.py","VisitOneDate.py","Event_DM_T.py"]


# for script in script:
#     try:
#         subprocess.run([python_path, script], check=True)
#         print(f"{script} executed successfully:")
#     except subprocess.CalledProcessError as e:
#         print(f"Error executing {script}: {e.stderr}")

from flask import Flask, render_template_string
import subprocess
import os
from dotenv import load_dotenv

app = Flask(__name__)

# Define the Python interpreter path
python_path = "python"

# Hardcoded list of Python scripts to execute
scripts = [
    "AuthT.py", "CreateCasebookT.py", "Event_IC_T.py",
    "VisitOneDate.py", "Event_DM_T.py"
]

@app.route('/run-all')
def run_all_scripts():
    """Execute all scripts sequentially and display results in the browser."""
    results = []
    
    for script in scripts:
        try:
            subprocess.run([python_path, script], check=True)
            results.append(f'<p style="color: green;">✅ {script} executed successfully.</p>')
        except subprocess.CalledProcessError as e:
            results.append(f'<p style="color: red;">❌ Error executing {script}: {str(e)}</p>')

    # Render results as an HTML page
    html_template = f"""
    <html>
    <head><title>Script Execution Results</title></head>
    <body>
        <h2>Execution Results</h2>
        {''.join(results)}
    </body>
    </html>
    """
    return render_template_string(html_template)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
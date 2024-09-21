from fastapi import FastAPI, HTTPException
import os
import subprocess
import sqlparse

app = FastAPI()


DBT_MODELS_PATH = "/home/anilkumar/DBT_Projects/sample_dbt_project/models"

@app.post("/create_model/")
async def create_model(model_name: str, sql_query: str, model_type: str = "raw_data"):
    # Define the folder path based on model type
    folder_path = os.path.join(DBT_MODELS_PATH, model_type)
    
    # Ensure the folder exists
    if not os.path.exists(folder_path):
        raise HTTPException(status_code=404, detail="Model type folder not found")
    
    # Format the incoming SQL query
    structured_query = sqlparse.format(sql_query, reindent=True, keyword_case='upper')

    # Create the full file path (ensure it ends with .sql)
    file_path = os.path.join(folder_path, f"{model_name}.sql")
    
    # Write the SQL query to the file
    with open(file_path, "w") as file:
        file.write(structured_query)
    
    # Git operations
    try:
        # Change to the DBT models directory
        os.chdir(DBT_MODELS_PATH)
        
        # Stage the new file
        subprocess.run(["git", "add", f"{model_type}/{model_name}.sql"], check=True)
        
        # Commit the change
        subprocess.run(["git", "commit", "-m", f"Add model {model_name}.sql"], check=True)
        
        # Push to the remote repository
        subprocess.run(["git", "push"], check=True)
    
    except subprocess.CalledProcessError as e:
        raise HTTPException(status_code=500, detail=f"Git operation failed: {str(e)}")

    return {"message": f"Model {model_name}.sql created and pushed to {model_type} folder"}
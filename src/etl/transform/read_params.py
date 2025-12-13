from etl.utils.utils_methods import load_params

if __name__ == "__main__":
    params = load_params()
    print("Project ID: ", params["bigquery"]["project_id"])

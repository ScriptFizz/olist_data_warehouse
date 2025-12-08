import typer
from typing_extensions import Annotated
from utils.utils_methods import load_params
from config.logconfig import logger

app = typer.Typer(help = "ETL: extract dataset from Kaggle")


def configure_kaggle_cli(
	username: Annotated[str, typer.Option(help = "Kaggle username")],
	key: Annotated[str, typer.Option(help = "Kaggle API key of the user")]
) -> None:
	"""
	Automatically configure Kaggle CLI
	
	Args:
		username (str): Kaggle username
		key (str): Kaggle API key.
	
	Returns:
		None:
	"""
	
	kaggle_dir = Path.home() / ".kaggle"
	kaggle_dir.mkdir(exist_ok = True)
	
	kaggle_config_file = kaggle_dir / "kaggle.json"
	
	if kaggle_config_file.exists():
		kaggle_config_file.unlink()
	
	kaggle_config_file.write_text(
		json.dumps({
			"username": username,
			"key": key
		})
	)
	
	# required permissions
	kaggle_config_file.chmod(0o600)
	
def ingest_data(
	out_dir: Annotated[str, typer.Option(help = "Directory where the data will be stored")], 
	dataset_name: Annotated[str, typer.Option(help = "Dataset identifier to download from Kaggle")]) -> None:
	"""
	Ingest Kaggle dataset (Require Kaggle credentials setup).

	Args:
		out_dir (str): location where the dataset is saved.
		dataset_name (str): identifier of the dataset to download (the username/dataset-name portion of https://www.kaggle.com/datasets/username/dataset-name)
		
	Returns:
		None:
	"""
	
	logger.info(f"Starting data ingestion into {out_dir}.")
	# Ensure the output director exists.
	Path(out_dir).mkdir(parents = True, exist_ok = True)
	
	cmd = [
		"kaggle", "datasets", "download",
		"-d", dataset_name,
		"-p", out_dir,
		"--unzip"
	]
	
	
	subprocess.run(cmd, check = True)
	logger.info(f"Data ingestion completed.")

@app.command()
def run(
	out_dir: Annotated[str, typer.Option(help = "Directory where the data will be stored")] = None, 
	dataset_name: Annotated[str, typer.Option(help = "Dataset identifier to download from Kaggle")] = None) -> None:
	"""
	Ingest Kaggle dataset (Require Kaggle credentials setup).

	Args:
		out_dir (str): location where the dataset is saved.
		dataset_name (str): identifier of the dataset to download (the username/dataset-name portion of https://www.kaggle.com/datasets/username/dataset-name)
		
	Returns:
		None:
	"""
	
	params = load_params()
	
	load_dotenv()
	kaggle_username = os.getenv("KAGGLE_USERNAME")
	kaggle_key = os.getenv("KAGGLE_KEY")
	
	if not kaggle_username or not kaggle_key:
		raise ValueError("KAGGLE_USERNAME or KAGGLE_KEY missing in .env file")
	
	configure_kaggle_cli(username = kaggle_username, key = kaggle_key)
	
	out_dir = out_dir or Path(params["path"]["raw_data_dir"])
	dataset_name = dataset_name or Path(params["dataset_name"])
	
	typer.echo(f"Extracting data files from {dataset_name} to {out_dir}")
	ingest_data(our_dir = out_dir, dataset_name = dataset_name)

if __name__ == "__main__":
	app()

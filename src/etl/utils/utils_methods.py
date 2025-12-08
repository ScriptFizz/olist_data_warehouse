import yaml

def load_params(path: str = "config/settings.yaml"):
	"""
	Read a configuration yaml file and return its data.
	
	Args:
		path (str): filepath of the configuration file.
	
	Returns:
		Dict (Python object that best fits the data): configuration data in a nested structure
	"""
	with open(path, "r") as f:
		return yaml.safe_load(f)



from dotenv import load_dotenv, find_dotenv

# Prioritize .env in Current Working Directory (CWD), fallback to library path
load_dotenv(find_dotenv(usecwd=True))
load_dotenv()

# knwstack package
__version__ = "0.1.0"

import logging

import dotenv

dotenv.load_dotenv()

logging.basicConfig(
    level=logging.DEBUG,
    filename="test.log",
    filemode="w",
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

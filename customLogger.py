import logging


logger = logging.getLogger(__name__)

# Create Handlers
stream = logging.StreamHandler()
writer = logging.FileHandler('logs.log')

# Set the level of the handler

# stream.setLevel(logging.INFO)
# writer.setLevel(logging.DEBUG)

# Create Formatter
s_format = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
w_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

stream.setFormatter(s_format)
writer.setFormatter(w_format)

# Add the handlers to the logger

logger.addHandler(stream)
logger.addHandler(writer)
logger.setLevel(logging.INFO)
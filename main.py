from roverdata.db import DataWarehouse
from taxes_increase.queries import queries_dict
from taxes_increase import utils

def main():
    logger = utils.setup_logger(__name__)
    conn = DataWarehouse()

    logger.info('fetching LTV data')
    LTV = conn.query(["LTV"])
    logger.info('fetching retransaction data')
    retrans = conn.query(["retrans"])

    return LTV, retrans
